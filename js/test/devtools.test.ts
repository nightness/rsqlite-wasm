// Tests for src/devtools.ts. The bridge logic is engine-independent — it
// just multiplexes calls onto whatever Database-shaped object you give it —
// so we mock the Database surface here instead of building a real WASM db.
//
// Real-engine integration (the bridge actually executing SQL on a live
// rsqlite-wasm Database) is exercised by the Brainwires OPFS extension's
// Playwright suite, which loads the wasm and exposes a real db.

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { exposeForDevtools, type ExposeForDevtoolsOptions } from "../src/devtools";

const GLOBAL_KEY = "__BRAINWIRES_RSQLITE_DEVTOOLS__";

interface MockDb {
  exec(sql: string, params?: unknown[]): number;
  execMany(sql: string): void;
  query<T = unknown>(sql: string, params?: unknown[]): T[];
  queryOne<T = unknown>(sql: string, params?: unknown[]): T | null;
  isClosed: boolean;
  // Mock metadata used by the test to assert the bridge actually called us
  log: Array<{ op: string; sql: string; params?: unknown[] }>;
}

function mockDb(): MockDb {
  const log: MockDb["log"] = [];
  let closed = false;
  return {
    log,
    get isClosed() {
      return closed;
    },
    set isClosed(v) {
      closed = v;
    },
    exec(sql, params) {
      log.push({ op: "exec", sql, params });
      return 1;
    },
    execMany(sql) {
      log.push({ op: "execMany", sql });
    },
    query<T>(sql: string, params?: unknown[]): T[] {
      log.push({ op: "query", sql, params });
      return [{ id: 1, name: "alice" }, { id: 2, name: "bob" }] as unknown as T[];
    },
    queryOne<T>(sql: string, params?: unknown[]): T | null {
      log.push({ op: "queryOne", sql, params });
      return ({ count: 42 } as unknown) as T;
    },
  } as unknown as MockDb;
}

beforeEach(() => {
  // ensure clean global between tests
  delete (globalThis as Record<string, unknown>)[GLOBAL_KEY];
});
afterEach(() => {
  delete (globalThis as Record<string, unknown>)[GLOBAL_KEY];
});

function bridge() {
  return (globalThis as Record<string, unknown>)[GLOBAL_KEY] as {
    v: number;
    listDbs(): string[];
    info(name: string): { name: string; changeCounter: number; closed: boolean } | null;
    invoke(name: string, op: string, sql: string, params?: unknown[]): number;
    poll(id: number): { pending: true } | { pending: false; ok: boolean; value?: unknown; error?: { message: string } };
  };
}

async function callOp(name: string, op: string, sql: string, params?: unknown[]) {
  const id = bridge().invoke(name, op, sql, params);
  // Bridge dispatches via Promise.resolve().then(...) so we wait one tick.
  await Promise.resolve();
  await Promise.resolve();
  const r = bridge().poll(id);
  if (r.pending) throw new Error("still pending after microtask drain");
  return r;
}

describe("exposeForDevtools", () => {
  it("installs the bridge global on first call", () => {
    expect((globalThis as Record<string, unknown>)[GLOBAL_KEY]).toBeUndefined();
    const db = mockDb();
    exposeForDevtools(db as unknown as Parameters<typeof exposeForDevtools>[0]);
    const root = bridge();
    expect(root).toBeDefined();
    expect(root.v).toBe(1);
    expect(root.listDbs()).toEqual(["main"]);
  });

  it("registers under custom names and lists them all", () => {
    exposeForDevtools(mockDb() as never, { name: "users" });
    exposeForDevtools(mockDb() as never, { name: "logs" });
    expect(bridge().listDbs().sort()).toEqual(["logs", "users"]);
  });

  it("opts.disabled is a no-op", () => {
    exposeForDevtools(mockDb() as never, { disabled: true });
    expect((globalThis as Record<string, unknown>)[GLOBAL_KEY]).toBeUndefined();
  });

  it("info() reports name + changeCounter + closed", () => {
    const db = mockDb();
    exposeForDevtools(db as never);
    expect(bridge().info("main")).toEqual({
      name: "main",
      changeCounter: 0,
      closed: false,
    });
    expect(bridge().info("missing")).toBeNull();
  });

  it("query round-trips through invoke/poll", async () => {
    const db = mockDb();
    exposeForDevtools(db as never);
    const r = await callOp("main", "query", "SELECT * FROM t", undefined);
    expect(r.pending).toBe(false);
    expect(r.ok).toBe(true);
    expect(r.value).toEqual([{ id: 1, name: "alice" }, { id: 2, name: "bob" }]);
    expect(db.log).toEqual([{ op: "query", sql: "SELECT * FROM t", params: undefined }]);
  });

  it("queryOne returns first row from mock", async () => {
    const db = mockDb();
    exposeForDevtools(db as never);
    const r = await callOp("main", "queryOne", "SELECT COUNT(*) FROM t");
    expect(r.ok).toBe(true);
    expect(r.value).toEqual({ count: 42 });
  });

  it("exec bumps changeCounter", async () => {
    const db = mockDb();
    exposeForDevtools(db as never);
    expect(bridge().info("main")?.changeCounter).toBe(0);
    await callOp("main", "exec", "UPDATE t SET name = 'x'", undefined);
    expect(bridge().info("main")?.changeCounter).toBe(1);
    await callOp("main", "exec", "INSERT INTO t VALUES (3, 'c')", undefined);
    expect(bridge().info("main")?.changeCounter).toBe(2);
    // Reads do not bump
    await callOp("main", "query", "SELECT * FROM t", undefined);
    expect(bridge().info("main")?.changeCounter).toBe(2);
  });

  it("execMany also bumps changeCounter", async () => {
    const db = mockDb();
    exposeForDevtools(db as never);
    await callOp("main", "execMany", "CREATE TABLE x(a); CREATE INDEX y ON x(a);");
    expect(bridge().info("main")?.changeCounter).toBe(1);
  });

  it("page-side direct call to db.exec ALSO bumps changeCounter", () => {
    // exposeForDevtools wraps db.exec/execMany so the user's own writes
    // are observable via the bridge's changeCounter.
    const db = mockDb();
    exposeForDevtools(db as never);
    db.exec("INSERT INTO t VALUES (10)");
    expect(bridge().info("main")?.changeCounter).toBe(1);
    db.execMany("UPDATE t SET a=1; UPDATE t SET a=2;");
    expect(bridge().info("main")?.changeCounter).toBe(2);
  });

  it("invoke for unknown db returns NotRegistered error", async () => {
    exposeForDevtools(mockDb() as never);
    const r = await callOp("nope", "query", "SELECT 1");
    expect(r.ok).toBe(false);
    expect(r.error?.message).toMatch(/not registered/);
  });

  it("invoke when db.isClosed returns Closed error", async () => {
    const db = mockDb();
    exposeForDevtools(db as never);
    db.isClosed = true;
    const r = await callOp("main", "query", "SELECT 1");
    expect(r.ok).toBe(false);
    expect(r.error?.message).toMatch(/closed/i);
  });

  it("propagates engine errors as poll() error", async () => {
    const db = mockDb();
    db.query = () => {
      throw new Error("syntax error near 'XYZ'");
    };
    exposeForDevtools(db as never);
    const r = await callOp("main", "query", "BAD SQL");
    expect(r.ok).toBe(false);
    expect(r.error?.message).toBe("syntax error near 'XYZ'");
  });

  it("re-exposing the same name swaps in the new db (HMR-friendly)", async () => {
    const db1 = mockDb();
    const db2 = mockDb();
    exposeForDevtools(db1 as never, { name: "live" });
    exposeForDevtools(db2 as never, { name: "live" });
    await callOp("live", "query", "SELECT 1");
    expect(db1.log).toEqual([]);
    expect(db2.log).toHaveLength(1);
  });

  it("release() removes the registration", () => {
    const release = exposeForDevtools(mockDb() as never, { name: "removable" });
    expect(bridge().listDbs()).toContain("removable");
    release();
    // Bridge global may or may not exist depending on whether other dbs
    // remain — when the last db is removed we tear it down.
    expect((globalThis as Record<string, unknown>)[GLOBAL_KEY]).toBeUndefined();
  });

  it("poll on an unknown id returns Expired error", () => {
    exposeForDevtools(mockDb() as never);
    const r = bridge().poll(99999);
    expect(r.pending).toBe(false);
    expect((r as { ok: boolean }).ok).toBe(false);
  });
});
