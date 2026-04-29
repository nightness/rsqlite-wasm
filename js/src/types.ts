export type SqlValue = null | number | string | Uint8Array;

export type BindParams = SqlValue[] | Record<string, SqlValue>;

export interface Row {
  [column: string]: SqlValue;
}

export interface DatabaseOptions {
  /** Storage backend. Defaults to auto-detection (OPFS > IndexedDB). */
  backend?: "memory" | "opfs" | "indexeddb";
}
