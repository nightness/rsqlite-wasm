//! `rtree` — multi-dimensional bounding-box storage with brute-force
//! overlap queries.
//!
//! ```sql
//! CREATE VIRTUAL TABLE places USING rtree(2);
//! INSERT INTO places VALUES (?min_x, ?max_x, ?min_y, ?max_y);
//!
//! -- All bounding boxes that overlap the query rectangle (0,10)x(0,10):
//! SELECT rowid FROM places
//! WHERE rtree_overlaps(min_x, max_x, min_y, max_y, 0, 10, 0, 10);
//! ```
//!
//! Like [`super::vec_index`], the on-the-tin API matches what a real R*-Tree
//! would expose so the algorithmic swap in v0.2 is API-compatible. For
//! v0.1 the lookup path is brute force — every row scanned, the user's
//! `WHERE` does the overlap test.
//!
//! Supports 1D through 5D, matching SQLite's R-Tree extension. Each
//! dimension contributes two columns: `min_<i>`, `max_<i>` (i = 0…dim-1).

use std::cell::RefCell;
use std::rc::Rc;

use rsqlite_storage::codec::Value;

use crate::error::{Error, Result};
use crate::types::Row;

use super::{Module, VirtualTable};

const MAX_DIM: usize = 5;

pub(super) struct RtreeModule;

impl Module for RtreeModule {
    fn name(&self) -> &str {
        "rtree"
    }

    fn create(&self, _table_name: &str, args: &[String]) -> Result<Rc<dyn VirtualTable>> {
        let dim = match args {
            [d] => d.parse::<usize>().map_err(|_| {
                Error::Other(format!(
                    "rtree: dim must be a positive integer (1..={MAX_DIM}), got {d:?}"
                ))
            })?,
            _ => {
                return Err(Error::Other(
                    "rtree: expected one argument (dim) — `USING rtree(2)`".into(),
                ));
            }
        };
        if dim == 0 || dim > MAX_DIM {
            return Err(Error::Other(format!(
                "rtree: dim must be 1..={MAX_DIM}, got {dim}"
            )));
        }
        let cols: Vec<String> = (0..dim)
            .flat_map(|i| [format!("min_{i}"), format!("max_{i}")])
            .collect();
        Ok(Rc::new(RtreeTable {
            dim,
            cols,
            // One row = `2 * dim` floats (min_0, max_0, min_1, max_1, …),
            // stored contiguously so per-row reads are offset slices.
            entries: RefCell::new(Vec::new()),
        }))
    }
}

pub(super) struct RtreeTable {
    dim: usize,
    cols: Vec<String>,
    entries: RefCell<Vec<f64>>,
}

impl RtreeTable {
    fn row_floats(&self) -> usize {
        self.dim * 2
    }

    /// Brute-force scan returning rowids whose bounding box overlaps the
    /// query box. Each box is `[min_0, max_0, min_1, max_1, …]`. Exposed
    /// for tests; future xBestIndex wiring would call this directly to
    /// skip the surrounding Filter wrap.
    pub fn overlapping(&self, query: &[f64]) -> Result<Vec<i64>> {
        if query.len() != self.row_floats() {
            return Err(Error::Other(format!(
                "rtree: query box has {} floats, table is {}D ({} expected)",
                query.len(),
                self.dim,
                self.row_floats()
            )));
        }
        let storage = self.entries.borrow();
        let stride = self.row_floats();
        let n = storage.len() / stride;
        let mut out = Vec::new();
        for i in 0..n {
            let row = &storage[i * stride..(i + 1) * stride];
            if boxes_overlap(self.dim, row, query) {
                out.push((i as i64) + 1);
            }
        }
        Ok(out)
    }
}

fn boxes_overlap(dim: usize, a: &[f64], b: &[f64]) -> bool {
    for d in 0..dim {
        let (a_min, a_max) = (a[d * 2], a[d * 2 + 1]);
        let (b_min, b_max) = (b[d * 2], b[d * 2 + 1]);
        // Disjoint along this dimension → boxes don't overlap.
        if a_max < b_min || b_max < a_min {
            return false;
        }
    }
    true
}

impl VirtualTable for RtreeTable {
    fn columns(&self) -> Vec<String> {
        self.cols.clone()
    }

    fn scan(&self) -> Result<Vec<Row>> {
        let storage = self.entries.borrow();
        let stride = self.row_floats();
        let n = storage.len() / stride;
        let mut rows = Vec::with_capacity(n);
        for i in 0..n {
            let slice = &storage[i * stride..(i + 1) * stride];
            let values: Vec<Value> = slice.iter().map(|f| Value::Real(*f)).collect();
            rows.push(Row::with_rowid(values, (i as i64) + 1));
        }
        Ok(rows)
    }

    fn insert(&self, values: &[Value]) -> Result<i64> {
        if values.len() != self.row_floats() {
            return Err(Error::Other(format!(
                "rtree: INSERT expects {} columns, got {}",
                self.row_floats(),
                values.len()
            )));
        }
        let mut row = Vec::with_capacity(self.row_floats());
        for (i, v) in values.iter().enumerate() {
            let f = match v {
                Value::Real(f) => *f,
                Value::Integer(n) => *n as f64,
                _ => {
                    return Err(Error::Other(format!(
                        "rtree: column {i} must be numeric (REAL or INTEGER)"
                    )));
                }
            };
            row.push(f);
        }
        // Sanity-check min ≤ max per dimension.
        for d in 0..self.dim {
            if row[d * 2] > row[d * 2 + 1] {
                return Err(Error::Other(format!(
                    "rtree: dim {d} has min ({}) > max ({})",
                    row[d * 2],
                    row[d * 2 + 1]
                )));
            }
        }
        let mut storage = self.entries.borrow_mut();
        storage.extend_from_slice(&row);
        Ok((storage.len() / self.row_floats()) as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_table(dim: usize) -> RtreeTable {
        let cols: Vec<String> = (0..dim)
            .flat_map(|i| [format!("min_{i}"), format!("max_{i}")])
            .collect();
        RtreeTable {
            dim,
            cols,
            entries: RefCell::new(Vec::new()),
        }
    }

    #[test]
    fn create_with_2d_arg() {
        let m = RtreeModule;
        let t = m.create("places", &["2".into()]).unwrap();
        assert_eq!(t.columns(), vec!["min_0", "max_0", "min_1", "max_1"]);
    }

    #[test]
    fn create_rejects_zero_or_too_many_dims() {
        let m = RtreeModule;
        assert!(m.create("t", &["0".into()]).is_err());
        assert!(m.create("t", &["6".into()]).is_err());
        assert!(m.create("t", &[]).is_err());
        assert!(m.create("t", &["abc".into()]).is_err());
    }

    #[test]
    fn insert_validates_min_le_max() {
        let table = fresh_table(2);
        // min ≤ max — accepted.
        table
            .insert(&[
                Value::Real(0.0),
                Value::Real(1.0),
                Value::Real(0.0),
                Value::Real(1.0),
            ])
            .unwrap();
        // min > max — rejected.
        assert!(table
            .insert(&[
                Value::Real(2.0),
                Value::Real(1.0),
                Value::Real(0.0),
                Value::Real(1.0),
            ])
            .is_err());
    }

    #[test]
    fn insert_rejects_wrong_column_count() {
        let table = fresh_table(2);
        assert!(table.insert(&[Value::Real(0.0), Value::Real(1.0)]).is_err());
    }

    #[test]
    fn overlapping_2d_finds_intersecting_boxes() {
        let table = fresh_table(2);
        // Three 2D boxes:
        //   1: (0..2) x (0..2)
        //   2: (5..6) x (5..6)
        //   3: (1..3) x (1..3) — overlaps box 1 and the query box
        for coords in &[
            [0.0, 2.0, 0.0, 2.0],
            [5.0, 6.0, 5.0, 6.0],
            [1.0, 3.0, 1.0, 3.0],
        ] {
            table
                .insert(&coords.iter().map(|f| Value::Real(*f)).collect::<Vec<_>>())
                .unwrap();
        }
        // Query (1.5..2.5) x (1.5..2.5) should hit boxes 1 and 3, not 2.
        let hits = table.overlapping(&[1.5, 2.5, 1.5, 2.5]).unwrap();
        assert_eq!(hits, vec![1, 3]);
    }

    #[test]
    fn overlapping_1d_works_too() {
        let table = fresh_table(1);
        table.insert(&[Value::Real(0.0), Value::Real(5.0)]).unwrap();
        table.insert(&[Value::Real(10.0), Value::Real(20.0)]).unwrap();
        let hits = table.overlapping(&[3.0, 4.0]).unwrap();
        assert_eq!(hits, vec![1]);
    }

    #[test]
    fn scan_round_trips() {
        let table = fresh_table(2);
        table
            .insert(&[
                Value::Real(1.0),
                Value::Real(2.0),
                Value::Real(3.0),
                Value::Real(4.0),
            ])
            .unwrap();
        let rows = table.scan().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].rowid, Some(1));
        assert_eq!(
            rows[0].values,
            vec![
                Value::Real(1.0),
                Value::Real(2.0),
                Value::Real(3.0),
                Value::Real(4.0),
            ]
        );
    }

    #[test]
    fn integer_inserts_coerce_to_float() {
        let table = fresh_table(1);
        table
            .insert(&[Value::Integer(0), Value::Integer(10)])
            .unwrap();
        let rows = table.scan().unwrap();
        assert_eq!(rows[0].values, vec![Value::Real(0.0), Value::Real(10.0)]);
    }
}
