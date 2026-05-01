//! `vec_index` — typed vector storage with brute-force similarity search.
//!
//! ```sql
//! CREATE VIRTUAL TABLE embeds USING vec_index(dim=384, metric=cosine);
//! INSERT INTO embeds VALUES (?vector_blob);
//! SELECT rowid, vec_distance_cosine(vector, ?) AS d
//! FROM embeds ORDER BY d LIMIT 10;
//! ```
//!
//! What this module provides over a plain `(rowid INTEGER PRIMARY KEY,
//! vector BLOB)` table:
//!
//! - **Typed declaration.** `dim=N, metric=M` are recorded at CREATE
//!   time. The `metric` parameter is exposed via `metric()` so other
//!   code can pick the right distance function; `dim` lets us reject
//!   wrong-shape inserts immediately.
//! - **Strict insert validation.** The `vector` blob must decode to
//!   exactly `dim` floats. Rejecting at write time keeps queries from
//!   surfacing "vector dimension mismatch" errors at scan time.
//! - **Stable storage layout.** Vectors are kept in a contiguous
//!   `Vec<f32>` of length `dim * row_count`, so per-row reads are a
//!   memcpy from a known offset rather than a btree traversal.
//!
//! For v0.1 the lookup path is brute force (every row scanned, sorted by
//! the user's `ORDER BY` clause). HNSW or another approximate-NN graph
//! is a future v0.2 swap behind the same module API.

use std::cell::RefCell;
use std::rc::Rc;

use rsqlite_storage::codec::Value;

use crate::error::{Error, Result};
use crate::types::Row;

use super::{Module, VirtualTable};

/// Distance metric declared by the user at CREATE time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VecMetric {
    Cosine,
    L2,
    Dot,
}

impl VecMetric {
    fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "cosine" => Some(VecMetric::Cosine),
            "l2" | "euclidean" => Some(VecMetric::L2),
            "dot" => Some(VecMetric::Dot),
            _ => None,
        }
    }
}

pub(super) struct VecIndexModule;

impl Module for VecIndexModule {
    fn name(&self) -> &str {
        "vec_index"
    }

    fn create(&self, _table_name: &str, args: &[String]) -> Result<Rc<dyn VirtualTable>> {
        // Args are `key=value` pairs separated by commas. Required: `dim`.
        // Optional: `metric` (defaults to `cosine`).
        let mut dim: Option<usize> = None;
        let mut metric = VecMetric::Cosine;
        for arg in args {
            let eq = arg.find('=').ok_or_else(|| {
                Error::Other(format!(
                    "vec_index: expected `key=value` argument, got {arg:?}"
                ))
            })?;
            let key = arg[..eq].trim().to_ascii_lowercase();
            let val = arg[eq + 1..].trim();
            match key.as_str() {
                "dim" => {
                    dim = Some(val.parse::<usize>().map_err(|_| {
                        Error::Other(format!(
                            "vec_index: dim must be a positive integer, got {val:?}"
                        ))
                    })?);
                }
                "metric" => {
                    metric = VecMetric::parse(val).ok_or_else(|| {
                        Error::Other(format!(
                            "vec_index: metric must be one of cosine, l2, dot — got {val:?}"
                        ))
                    })?;
                }
                _ => {
                    return Err(Error::Other(format!(
                        "vec_index: unknown argument key {key:?}"
                    )));
                }
            }
        }
        let dim = dim.ok_or_else(|| {
            Error::Other("vec_index: missing required argument `dim=N`".into())
        })?;
        if dim == 0 {
            return Err(Error::Other("vec_index: dim must be > 0".into()));
        }
        Ok(Rc::new(VecIndexTable {
            dim,
            metric,
            // Contiguous storage: row k's vector lives at
            // `vectors[k*dim .. (k+1)*dim]`.
            vectors: RefCell::new(Vec::new()),
        }))
    }
}

pub(super) struct VecIndexTable {
    dim: usize,
    metric: VecMetric,
    vectors: RefCell<Vec<f32>>,
}

impl VecIndexTable {
    /// Number of stored vectors.
    fn len(&self) -> usize {
        self.vectors.borrow().len() / self.dim
    }

    /// Brute-force k-nearest-neighbor search. Returns `(rowid,
    /// distance)` pairs sorted by ascending distance. `k` of 0 returns
    /// every stored row. Exposed for tests and for future TVF wiring.
    pub fn nearest(&self, query: &[f32], k: usize) -> Result<Vec<(i64, f64)>> {
        if query.len() != self.dim {
            return Err(Error::Other(format!(
                "vec_index: query has {} dims, table declared {}",
                query.len(),
                self.dim
            )));
        }
        let storage = self.vectors.borrow();
        let n = storage.len() / self.dim;
        let mut scored: Vec<(i64, f64)> = (0..n)
            .map(|i| {
                let stored = &storage[i * self.dim..(i + 1) * self.dim];
                let d = match self.metric {
                    VecMetric::Cosine => crate::eval_helpers::cosine_distance(stored, query),
                    VecMetric::L2 => crate::eval_helpers::l2_distance(stored, query),
                    VecMetric::Dot => -dot_product(stored, query),
                };
                ((i as i64) + 1, d)
            })
            .collect();
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        if k > 0 && scored.len() > k {
            scored.truncate(k);
        }
        Ok(scored)
    }
}

fn dot_product(a: &[f32], b: &[f32]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(x, y)| (*x as f64) * (*y as f64))
        .sum()
}

impl VirtualTable for VecIndexTable {
    fn columns(&self) -> Vec<String> {
        vec!["vector".to_string()]
    }

    fn scan(&self) -> Result<Vec<Row>> {
        let storage = self.vectors.borrow();
        let n = storage.len() / self.dim;
        let mut rows = Vec::with_capacity(n);
        for i in 0..n {
            let bytes = f32_slice_to_blob(&storage[i * self.dim..(i + 1) * self.dim]);
            rows.push(Row::with_rowid(vec![Value::Blob(bytes)], (i as i64) + 1));
        }
        Ok(rows)
    }

    fn insert(&self, values: &[Value]) -> Result<i64> {
        if values.len() != 1 {
            return Err(Error::Other(format!(
                "vec_index: INSERT expects 1 column (vector), got {}",
                values.len()
            )));
        }
        let blob = match &values[0] {
            Value::Blob(b) => b.as_slice(),
            Value::Null => {
                return Err(Error::Other(
                    "vec_index: vector column does not accept NULL".into(),
                ));
            }
            _ => {
                return Err(Error::Other(
                    "vec_index: vector column requires a BLOB value".into(),
                ));
            }
        };
        let parsed = crate::eval_helpers::blob_to_f32_vec(blob)?;
        if parsed.len() != self.dim {
            return Err(Error::Other(format!(
                "vec_index: vector dimension mismatch — got {}, expected {}",
                parsed.len(),
                self.dim
            )));
        }
        let mut storage = self.vectors.borrow_mut();
        storage.extend_from_slice(&parsed);
        Ok((storage.len() / self.dim) as i64)
    }
}

fn f32_slice_to_blob(slice: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(slice.len() * 4);
    for v in slice {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vec_blob(values: &[f32]) -> Vec<u8> {
        f32_slice_to_blob(values)
    }

    fn fresh_table(dim: usize, metric: VecMetric) -> VecIndexTable {
        VecIndexTable {
            dim,
            metric,
            vectors: RefCell::new(Vec::new()),
        }
    }

    #[test]
    fn create_with_dim_and_metric_args() {
        let m = VecIndexModule;
        let t = m
            .create("e", &["dim=3".into(), "metric=l2".into()])
            .unwrap();
        // Empty until populated.
        assert_eq!(t.scan().unwrap().len(), 0);
    }

    #[test]
    fn create_rejects_missing_dim() {
        let m = VecIndexModule;
        assert!(m.create("e", &[]).is_err());
        assert!(m.create("e", &["metric=cosine".into()]).is_err());
    }

    #[test]
    fn insert_validates_dimension() {
        let m = VecIndexModule;
        let t = m.create("e", &["dim=3".into()]).unwrap();
        // Right shape — accepted.
        t.insert(&[Value::Blob(vec_blob(&[1.0, 0.0, 0.0]))]).unwrap();
        // Wrong shape — rejected at write time.
        assert!(t
            .insert(&[Value::Blob(vec_blob(&[1.0, 0.0]))])
            .is_err());
    }

    #[test]
    fn insert_rejects_null_and_non_blob() {
        let m = VecIndexModule;
        let t = m.create("e", &["dim=2".into()]).unwrap();
        assert!(t.insert(&[Value::Null]).is_err());
        assert!(t.insert(&[Value::Integer(7)]).is_err());
    }

    #[test]
    fn nearest_brute_force_cosine() {
        let table = fresh_table(3, VecMetric::Cosine);
        // 3 unit vectors along axes.
        table.insert(&[Value::Blob(vec_blob(&[1.0, 0.0, 0.0]))]).unwrap();
        table.insert(&[Value::Blob(vec_blob(&[0.0, 1.0, 0.0]))]).unwrap();
        table.insert(&[Value::Blob(vec_blob(&[0.0, 0.0, 1.0]))]).unwrap();

        let neighbors = table.nearest(&[0.9, 0.1, 0.0], 2).unwrap();
        assert_eq!(neighbors.len(), 2);
        // Nearest is rowid 1 (1,0,0) — small angle to (0.9, 0.1, 0.0).
        assert_eq!(neighbors[0].0, 1);
        assert!(neighbors[0].1 < neighbors[1].1);
    }

    #[test]
    fn nearest_l2_distances() {
        let table = fresh_table(2, VecMetric::L2);
        table.insert(&[Value::Blob(vec_blob(&[0.0, 0.0]))]).unwrap();
        table.insert(&[Value::Blob(vec_blob(&[3.0, 4.0]))]).unwrap();
        table.insert(&[Value::Blob(vec_blob(&[1.0, 1.0]))]).unwrap();

        let neighbors = table.nearest(&[0.0, 0.0], 0).unwrap();
        assert_eq!(neighbors.len(), 3);
        assert_eq!(neighbors[0].0, 1); // distance 0
        assert_eq!(neighbors[1].0, 3); // sqrt(2) ≈ 1.41
        assert_eq!(neighbors[2].0, 2); // 5.0
    }

    #[test]
    fn nearest_dot_metric_returns_largest_dot_first() {
        let table = fresh_table(2, VecMetric::Dot);
        table.insert(&[Value::Blob(vec_blob(&[1.0, 0.0]))]).unwrap(); // dot with (2,1) = 2
        table.insert(&[Value::Blob(vec_blob(&[0.5, 1.0]))]).unwrap(); // dot = 2
        table.insert(&[Value::Blob(vec_blob(&[2.0, 1.0]))]).unwrap(); // dot = 5

        let neighbors = table.nearest(&[2.0, 1.0], 1).unwrap();
        assert_eq!(neighbors.len(), 1);
        // Largest dot product wins — that's rowid 3.
        assert_eq!(neighbors[0].0, 3);
    }

    #[test]
    fn scan_round_trips_inserted_vectors() {
        let m = VecIndexModule;
        let t = m.create("e", &["dim=2".into()]).unwrap();
        t.insert(&[Value::Blob(vec_blob(&[1.5, 2.5]))]).unwrap();
        t.insert(&[Value::Blob(vec_blob(&[-0.5, 7.0]))]).unwrap();
        let rows = t.scan().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].rowid, Some(1));
        assert_eq!(rows[1].rowid, Some(2));
        if let Value::Blob(b) = &rows[0].values[0] {
            let decoded = crate::eval_helpers::blob_to_f32_vec(b).unwrap();
            assert_eq!(decoded, vec![1.5, 2.5]);
        } else {
            panic!("expected blob");
        }
    }

    #[test]
    fn unknown_arg_is_an_error() {
        let m = VecIndexModule;
        assert!(m
            .create("e", &["dim=3".into(), "bogus=1".into()])
            .is_err());
        assert!(m
            .create("e", &["dim=3".into(), "metric=manhattan".into()])
            .is_err());
    }
}
