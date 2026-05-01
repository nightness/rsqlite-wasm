//! Virtual table foundation.
//!
//! A *virtual table* is a table whose rows aren't stored in the engine's
//! btrees — instead they're produced on demand by a [`VirtualTable`]
//! implementation supplied by a *module*. SQLite uses the same pattern for
//! FTS5, R-Tree, and many extensions.
//!
//! For v0.1 the surface is deliberately minimal:
//!
//! - **No `xBestIndex`.** Every scan is a full enumeration via
//!   [`VirtualTable::scan`]; the executor wraps the result in a Filter
//!   for any `WHERE` clause.
//! - **Read-only.** No `xUpdate` — virtual tables can't be the target of
//!   INSERT / UPDATE / DELETE yet.
//! - **In-memory column metadata.** A module declares its column names
//!   up front via [`Module::columns`]; types are inferred at scan time.
//!
//! That's enough surface to host built-in modules (the bundled `series`
//! generator) and to give external add-ons (FTS5, R-Tree, HNSW vector
//! index) somewhere to land.

use std::collections::HashMap;
use std::cell::RefCell;
use std::rc::Rc;

use rsqlite_storage::codec::Value;

use crate::error::Result;
use crate::types::Row;

/// Definition of a virtual-table module: how it parses its CREATE
/// VIRTUAL TABLE arguments and what columns its tables expose.
pub trait Module: 'static {
    /// Module name, matched case-insensitively against `USING <name>(...)`.
    fn name(&self) -> &str;

    /// Build a virtual-table instance for one `CREATE VIRTUAL TABLE`
    /// statement. `args` are the comma-separated tokens between the
    /// parentheses, with whitespace trimmed.
    fn create(&self, table_name: &str, args: &[String]) -> Result<Box<dyn VirtualTable>>;
}

/// A live virtual table. Cloned/recreated cheaply (modules typically
/// hold an Rc<inner>) — the planner asks for a fresh handle per scan.
pub trait VirtualTable {
    /// Names of the columns this table exposes, in declaration order.
    /// Matches the column list `xColumn` would project for SQLite.
    fn columns(&self) -> &[String];

    /// Produce all rows. The executor may wrap the result in a Filter
    /// if there's a WHERE clause; a future `xBestIndex` hook would let
    /// the module push filters down itself.
    fn scan(&self) -> Result<Vec<Row>>;
}

thread_local! {
    static MODULE_REGISTRY: RefCell<HashMap<String, Rc<dyn Module>>> =
        RefCell::new(default_modules());
}

fn default_modules() -> HashMap<String, Rc<dyn Module>> {
    let mut map: HashMap<String, Rc<dyn Module>> = HashMap::new();
    let series = Rc::new(SeriesModule) as Rc<dyn Module>;
    map.insert("series".to_string(), series.clone());
    map.insert("generate_series".to_string(), series);
    map
}

/// Register a virtual-table module by name. Names are stored
/// lowercased and matched case-insensitively. Re-registering replaces
/// the previous definition.
pub fn register_module(module: Rc<dyn Module>) {
    let key = module.name().to_ascii_lowercase();
    MODULE_REGISTRY.with(|r| {
        r.borrow_mut().insert(key, module);
    });
}

/// Look up a module by name (case-insensitive). Returns `None` if no
/// module with that name is registered.
pub fn lookup_module(name: &str) -> Option<Rc<dyn Module>> {
    let key = name.to_ascii_lowercase();
    MODULE_REGISTRY.with(|r| r.borrow().get(&key).cloned())
}

/// Drop every registered module and reinstate the built-in defaults.
/// Useful when tests register stub modules and want a clean slate.
pub fn reset_to_defaults() {
    MODULE_REGISTRY.with(|r| *r.borrow_mut() = default_modules());
}

// ── Built-in: `series` ────────────────────────────────────────────────

/// Generate-series style enumerator. Mirrors SQLite's `generate_series`
/// extension: `SELECT * FROM series(start, stop[, step])` yields one
/// row per integer in the half-open range.
struct SeriesModule;

impl Module for SeriesModule {
    fn name(&self) -> &str {
        "series"
    }

    fn create(&self, _table_name: &str, args: &[String]) -> Result<Box<dyn VirtualTable>> {
        let parse_int = |s: &str| -> Result<i64> {
            s.parse::<i64>().map_err(|_| {
                crate::error::Error::Other(format!(
                    "series module: argument {s:?} is not an integer"
                ))
            })
        };
        let (start, stop, step) = match args {
            [a, b] => (parse_int(a)?, parse_int(b)?, 1i64),
            [a, b, c] => (parse_int(a)?, parse_int(b)?, parse_int(c)?),
            _ => {
                return Err(crate::error::Error::Other(
                    "series module: expected 2 or 3 integer arguments".into(),
                ));
            }
        };
        Ok(Box::new(SeriesTable { start, stop, step }))
    }
}

struct SeriesTable {
    start: i64,
    stop: i64,
    step: i64,
}

impl VirtualTable for SeriesTable {
    fn columns(&self) -> &[String] {
        // Static column list — every series instance exposes one column,
        // `value`. Matches SQLite's generate_series schema (without the
        // optional `start`/`stop`/`step` reflection columns).
        static COLS: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
        COLS.get_or_init(|| vec!["value".to_string()])
    }

    fn scan(&self) -> Result<Vec<Row>> {
        if self.step == 0 {
            return Err(crate::error::Error::Other(
                "series module: step must be non-zero".into(),
            ));
        }
        let mut rows = Vec::new();
        let mut v = self.start;
        if self.step > 0 {
            while v <= self.stop {
                rows.push(Row::with_rowid(vec![Value::Integer(v)], v));
                v += self.step;
            }
        } else {
            while v >= self.stop {
                rows.push(Row::with_rowid(vec![Value::Integer(v)], v));
                v += self.step;
            }
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn series_two_args_default_step() {
        reset_to_defaults();
        let m = lookup_module("series").unwrap();
        let t = m.create("s", &["1".into(), "5".into()]).unwrap();
        let rows = t.scan().unwrap();
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0].values[0], Value::Integer(1));
        assert_eq!(rows[4].values[0], Value::Integer(5));
    }

    #[test]
    fn series_three_args_negative_step() {
        let m = lookup_module("generate_series").unwrap();
        let t = m
            .create("s", &["10".into(), "0".into(), "-2".into()])
            .unwrap();
        let rows = t.scan().unwrap();
        let vs: Vec<i64> = rows
            .iter()
            .filter_map(|r| {
                if let Value::Integer(n) = r.values[0] {
                    Some(n)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(vs, vec![10, 8, 6, 4, 2, 0]);
    }

    #[test]
    fn series_zero_step_errors() {
        let m = lookup_module("series").unwrap();
        let t = m
            .create("s", &["1".into(), "5".into(), "0".into()])
            .unwrap();
        assert!(t.scan().is_err());
    }

    #[test]
    fn unknown_module_returns_none() {
        reset_to_defaults();
        assert!(lookup_module("not_a_real_module").is_none());
    }

    #[test]
    fn registered_custom_module_is_found() {
        struct Stub;
        impl Module for Stub {
            fn name(&self) -> &str {
                "stub_module"
            }
            fn create(&self, _t: &str, _a: &[String]) -> Result<Box<dyn VirtualTable>> {
                Err(crate::error::Error::Other("not implemented".into()))
            }
        }
        register_module(Rc::new(Stub));
        assert!(lookup_module("stub_module").is_some());
        assert!(lookup_module("STUB_MODULE").is_some());
        reset_to_defaults();
        assert!(lookup_module("stub_module").is_none());
    }
}
