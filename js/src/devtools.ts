// Brainwires OPFS DevTools bridge.
//
// The Brainwires OPFS Chrome extension can browse and edit any OPFS-backed
// SQLite database from a DevTools panel. By default it loads a *snapshot* of
// the shards from disk, which means: while the inspected page holds an open
// SyncAccessHandle (i.e. the database is "live"), the panel cannot write to
// the file — OPFS sync handles are exclusive.
//
// `exposeForDevtools(db)` opts a Database in to a postMessage-style bridge
// that lets the panel route reads, writes and DDL through the page's own
// Database instance. There's no second handle, no lock conflict, and the
// page sees writes immediately because they execute on its engine.
//
// Bridge protocol (window.__BRAINWIRES_RSQLITE_DEVTOOLS__):
//   - .v                  — protocol version (current: 1)
//   - .listDbs()          — string[] of registered db names
//   - .info(name)         — { name, changeCounter, closed }
//   - .invoke(name, op, sql, params?) → integer invocation id
//                           op ∈ { 'query', 'queryOne', 'exec', 'execMany' }
//   - .poll(id)           → { pending: true } | { pending: false, ok, value, error }
//
// The page-side scheduler is microtask-based so the bridge is non-blocking.
// `changeCounter` increments on every successful exec/execMany so the panel
// can poll for "did the page write since I last looked" and refresh.

import type { Row, SqlValue } from "./types.js";

/**
 * Structural type that both {@link import("./index.js").Database} (sync)
 * and {@link import("./worker-proxy.js").WorkerDatabase} (Promise-returning)
 * satisfy. Either flavour can be exposed through the bridge.
 */
export interface DevtoolsDatabase {
  exec(sql: string, params?: SqlValue[]): number | Promise<number>;
  execMany(sql: string): void | Promise<void>;
  query<T extends Row = Row>(sql: string, params?: SqlValue[]): T[] | Promise<T[]>;
  queryOne<T extends Row = Row>(sql: string, params?: SqlValue[]): (T | null) | Promise<T | null>;
  readonly isClosed: boolean;
}

const GLOBAL_KEY = "__BRAINWIRES_RSQLITE_DEVTOOLS__" as const;
const PROTOCOL_VERSION = 1;

interface PendingResult {
  pending: true;
}
interface SuccessResult {
  pending: false;
  ok: true;
  value: unknown;
}
interface FailureResult {
  pending: false;
  ok: false;
  error: { message: string; code?: string };
}
type PollResult = PendingResult | SuccessResult | FailureResult;

interface RegisteredDb {
  name: string;
  db: DevtoolsDatabase;
  changeCounter: number;
}

export interface BridgeRoot {
  v: typeof PROTOCOL_VERSION;
  listDbs(): string[];
  info(name: string): { name: string; changeCounter: number; closed: boolean } | null;
  invoke(name: string, op: BridgeOp, sql: string, params?: SqlValue[]): number;
  poll(id: number): PollResult;
}

type BridgeOp = "query" | "queryOne" | "exec" | "execMany";

interface InternalState {
  byName: Map<string, RegisteredDb>;
  results: Map<number, PollResult>;
  nextId: number;
}

export interface ExposeForDevtoolsOptions {
  /** Display name used by the DevTools panel to reference this database.
   *  Multiple databases can be exposed under different names. Defaults to
   *  `"main"`. */
  name?: string;
  /** When true, this call is a no-op. Useful for production builds where
   *  you want the same source code to compile out the bridge:
   *  `exposeForDevtools(db, { disabled: import.meta.env.PROD })`. */
  disabled?: boolean;
}

/**
 * Expose a {@link Database} to the Brainwires OPFS DevTools extension.
 *
 * Call once per `Database` you want the panel to be able to read and write
 * through. The exposed db is wrapped so its own writes also bump a change
 * counter; the panel polls this to auto-refresh when your app modifies
 * data behind its back.
 *
 * Idempotent: re-exposing under the same name swaps in the new db
 * (handy across hot module reloads).
 *
 * Returns a `release()` function that removes the registration.
 *
 * @example
 *   import { Database, exposeForDevtools } from 'rsqlite-wasm';
 *
 *   const db = await Database.open('chat', { backend: 'opfs' });
 *   exposeForDevtools(db, { name: 'chat' });  // panel: shows "● live (chat)"
 *
 *   // Tree-shake out in production:
 *   exposeForDevtools(db, { disabled: process.env.NODE_ENV === 'production' });
 */
export function exposeForDevtools(
  db: DevtoolsDatabase,
  options?: ExposeForDevtoolsOptions
): () => void {
  if (options?.disabled) return () => {};
  if (typeof globalThis === "undefined") return () => {};
  // Use globalThis to support workers + main thread; in workers there's no
  // DevTools panel to talk to, but exposing is a harmless no-op.
  const w = globalThis as Record<string, unknown>;

  const name = options?.name ?? "main";

  let root = w[GLOBAL_KEY] as BridgeRoot | undefined;
  let state: InternalState;
  if (root && (root as BridgeRoot & { __state?: InternalState }).__state) {
    // Bridge already installed by a previous call — share its state.
    state = (root as BridgeRoot & { __state: InternalState }).__state;
  } else {
    state = { byName: new Map(), results: new Map(), nextId: 1 };
    root = installBridge(state);
    Object.defineProperty(w, GLOBAL_KEY, {
      value: root,
      configurable: true,
      writable: false,
      enumerable: false,
    });
  }

  // Wrap exec/execMany so the page's own writes bump changeCounter.
  // Tolerates both sync (Database) and Promise-returning (WorkerDatabase)
  // signatures. We only wrap once per db, even on re-exposure under a new name.
  const wrappedKey = "__bwOpfsWrapped" as const;
  if (!(db as unknown as Record<string, boolean>)[wrappedKey]) {
    const origExec = db.exec.bind(db) as DevtoolsDatabase["exec"];
    const origExecMany = db.execMany.bind(db) as DevtoolsDatabase["execMany"];
    const bumpAfter = <T,>(r: T | Promise<T>): T | Promise<T> => {
      if (r && typeof (r as { then?: unknown }).then === "function") {
        return (r as Promise<T>).then((v) => {
          const entry = state.byName.get(name);
          if (entry) entry.changeCounter++;
          return v;
        });
      }
      const entry = state.byName.get(name);
      if (entry) entry.changeCounter++;
      return r;
    };
    (db as { exec: DevtoolsDatabase["exec"] }).exec = function (
      sql: string,
      params?: SqlValue[],
    ) {
      return bumpAfter(origExec(sql, params));
    };
    (db as { execMany: DevtoolsDatabase["execMany"] }).execMany = function (
      sql: string,
    ) {
      return bumpAfter(origExecMany(sql));
    };
    Object.defineProperty(db, wrappedKey, {
      value: true,
      configurable: false,
      writable: false,
      enumerable: false,
    });
  }

  const entry: RegisteredDb = { name, db, changeCounter: 0 };
  state.byName.set(name, entry);

  return function release() {
    if (state.byName.get(name) === entry) state.byName.delete(name);
    if (state.byName.size === 0) {
      delete w[GLOBAL_KEY];
    }
  };
}

function installBridge(state: InternalState): BridgeRoot {
  const root: BridgeRoot & { __state: InternalState } = {
    v: PROTOCOL_VERSION,
    __state: state,
    listDbs(): string[] {
      return Array.from(state.byName.keys());
    },
    info(name: string) {
      const e = state.byName.get(name);
      if (!e) return null;
      return {
        name: e.name,
        changeCounter: e.changeCounter,
        closed: e.db.isClosed,
      };
    },
    invoke(name: string, op: BridgeOp, sql: string, params?: SqlValue[]): number {
      const id = state.nextId++;
      state.results.set(id, { pending: true });
      const entry = state.byName.get(name);
      // Run in a microtask so the caller gets the id back synchronously
      // and can immediately start polling. Awaits the db method so both
      // sync (Database) and Promise-returning (WorkerDatabase) implementations
      // are supported transparently.
      Promise.resolve().then(async () => {
        if (!entry) {
          state.results.set(id, {
            pending: false,
            ok: false,
            error: { message: `db not registered: ${name}`, code: "NotRegistered" },
          });
          return;
        }
        if (entry.db.isClosed) {
          state.results.set(id, {
            pending: false,
            ok: false,
            error: { message: "db is closed", code: "Closed" },
          });
          return;
        }
        try {
          let value: unknown;
          if (op === "query") value = await entry.db.query<Row>(sql, params);
          else if (op === "queryOne")
            value = await entry.db.queryOne<Row>(sql, params);
          else if (op === "exec") value = await entry.db.exec(sql, params);
          else if (op === "execMany") {
            await entry.db.execMany(sql);
            value = undefined;
          } else {
            throw new Error(`unknown op: ${op}`);
          }
          state.results.set(id, { pending: false, ok: true, value });
        } catch (e) {
          state.results.set(id, {
            pending: false,
            ok: false,
            error: { message: errorMessage(e) },
          });
        }
      });
      return id;
    },
    poll(id: number): PollResult {
      const r = state.results.get(id);
      if (!r) {
        return {
          pending: false,
          ok: false,
          error: { message: "result expired", code: "Expired" },
        };
      }
      if (!r.pending) state.results.delete(id);
      return r;
    },
  };
  return root;
}

function errorMessage(e: unknown): string {
  if (e instanceof Error) return e.message;
  return String(e);
}
