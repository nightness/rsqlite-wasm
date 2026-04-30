export type SqlValue = null | number | string | Uint8Array;

export type BindParams = SqlValue[] | Record<string, SqlValue>;

export interface Row {
  [column: string]: SqlValue;
}

export interface DatabaseOptions {
  /** Storage backend. Defaults to auto-detection (OPFS > IndexedDB). */
  backend?: "memory" | "opfs" | "indexeddb";
  /** Explicit URL for the worker script. When omitted, resolved via
   *  `new URL("./worker.js", import.meta.url)`. Bundlers that inline
   *  worker-proxy.js break `import.meta.url` resolution — pass the
   *  correct URL to work around that. */
  workerUrl?: string | URL;
}
