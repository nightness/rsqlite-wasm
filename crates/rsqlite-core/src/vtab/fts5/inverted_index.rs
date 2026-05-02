//! In-memory inverted index per FTS5 column.
//!
//! Layout:
//!
//! - `tokens`: term → (rowid → posting list of positions).
//! - `doc_lengths`: rowid → token count of the indexed document.
//! - `total_docs`: count of distinct rowids that have any postings,
//!   used for IDF in BM25.
//!
//! All operations are O(token-count) per document; lookup is O(1) on
//! the term followed by O(rowid-count) iteration.
//!
//! Persistence: [`InvertedIndex::serialize`] emits a length-prefixed
//! binary blob; [`InvertedIndex::deserialize`] reconstructs the index.
//! The format is compact enough for one-shot reads at open and is
//! versioned so the layout can evolve.

use std::collections::{BTreeMap, HashMap};

use super::tokenizer::Token;

pub type RowId = i64;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PostingList {
    pub positions: Vec<u32>,
}

/// Inverted index for a single column.
#[derive(Debug, Clone, Default)]
pub struct InvertedIndex {
    pub tokens: HashMap<String, BTreeMap<RowId, PostingList>>,
    pub doc_lengths: HashMap<RowId, u32>,
    pub total_docs: u32,
}

impl InvertedIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert (or replace) a document's token stream. Removes any
    /// previous postings for `rowid` first so updates roundtrip.
    pub fn upsert(&mut self, rowid: RowId, tokens: &[Token]) {
        if self.doc_lengths.contains_key(&rowid) {
            self.remove(rowid);
        }
        if tokens.is_empty() {
            self.doc_lengths.insert(rowid, 0);
            self.total_docs = self.total_docs.saturating_add(1);
            return;
        }
        for tok in tokens {
            let entry = self
                .tokens
                .entry(tok.text.clone())
                .or_default()
                .entry(rowid)
                .or_default();
            entry.positions.push(tok.position);
        }
        self.doc_lengths.insert(rowid, tokens.len() as u32);
        self.total_docs = self.total_docs.saturating_add(1);
    }

    /// Drop every posting for `rowid`. Idempotent.
    pub fn remove(&mut self, rowid: RowId) {
        if self.doc_lengths.remove(&rowid).is_none() {
            return;
        }
        // Iterate over a snapshot of the keys so we can mutate the
        // outer map safely.
        let keys: Vec<String> = self.tokens.keys().cloned().collect();
        for k in keys {
            if let Some(map) = self.tokens.get_mut(&k) {
                map.remove(&rowid);
                if map.is_empty() {
                    self.tokens.remove(&k);
                }
            }
        }
        self.total_docs = self.total_docs.saturating_sub(1);
    }

    /// Fetch the posting list for `term` exactly. Empty slice if absent.
    pub fn postings(&self, term: &str) -> Vec<(RowId, &PostingList)> {
        match self.tokens.get(term) {
            None => Vec::new(),
            Some(map) => map.iter().map(|(r, pl)| (*r, pl)).collect(),
        }
    }

    /// All terms beginning with `prefix`, with their posting lists.
    pub fn prefix_postings(&self, prefix: &str) -> Vec<(String, Vec<(RowId, PostingList)>)> {
        let mut out = Vec::new();
        for (term, map) in &self.tokens {
            if term.starts_with(prefix) {
                let entries: Vec<(RowId, PostingList)> =
                    map.iter().map(|(r, pl)| (*r, pl.clone())).collect();
                out.push((term.clone(), entries));
            }
        }
        out
    }

    /// Document frequency: how many distinct rowids contain `term`.
    pub fn doc_frequency(&self, term: &str) -> u32 {
        self.tokens.get(term).map(|m| m.len() as u32).unwrap_or(0)
    }

    /// Average document length (in tokens), or 0 when empty.
    pub fn avg_doc_length(&self) -> f64 {
        if self.doc_lengths.is_empty() {
            return 0.0;
        }
        let total: u64 = self.doc_lengths.values().map(|n| *n as u64).sum();
        total as f64 / self.doc_lengths.len() as f64
    }

    // ── Serialization ─────────────────────────────────────────────────

    /// Format magic + version. Bump `VERSION` whenever the layout
    /// changes so old blobs surface a clean error rather than reading
    /// junk.
    const MAGIC: &'static [u8; 4] = b"FTS5";
    const VERSION: u8 = 1;

    /// Serialize the entire index to a single blob. Layout:
    ///
    /// ```text
    /// magic (4) | version (1) | total_docs (u32 LE) |
    /// doc_count (u32 LE) [rowid (i64 LE) | length (u32 LE)]*
    /// term_count (u32 LE) [
    ///   term_len (u32 LE) | term_bytes
    ///   posting_count (u32 LE) [
    ///     rowid (i64 LE)
    ///     pos_count (u32 LE) [pos (u32 LE)]*
    ///   ]*
    /// ]*
    /// ```
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(Self::MAGIC);
        buf.push(Self::VERSION);
        buf.extend_from_slice(&self.total_docs.to_le_bytes());

        let dl = &self.doc_lengths;
        buf.extend_from_slice(&(dl.len() as u32).to_le_bytes());
        // Sort for determinism — makes round-trip tests stable.
        let mut dl_sorted: Vec<(RowId, u32)> = dl.iter().map(|(r, l)| (*r, *l)).collect();
        dl_sorted.sort_by_key(|(r, _)| *r);
        for (r, l) in dl_sorted {
            buf.extend_from_slice(&r.to_le_bytes());
            buf.extend_from_slice(&l.to_le_bytes());
        }

        buf.extend_from_slice(&(self.tokens.len() as u32).to_le_bytes());
        let mut term_keys: Vec<&String> = self.tokens.keys().collect();
        term_keys.sort();
        for term in term_keys {
            let bytes = term.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
            let map = &self.tokens[term];
            buf.extend_from_slice(&(map.len() as u32).to_le_bytes());
            for (rowid, pl) in map {
                buf.extend_from_slice(&rowid.to_le_bytes());
                buf.extend_from_slice(&(pl.positions.len() as u32).to_le_bytes());
                for p in &pl.positions {
                    buf.extend_from_slice(&p.to_le_bytes());
                }
            }
        }
        buf
    }

    pub fn deserialize(blob: &[u8]) -> Result<Self, String> {
        let mut r = Reader { buf: blob, pos: 0 };
        let magic = r.take(4)?;
        if magic != *Self::MAGIC {
            return Err("fts5 index: bad magic".into());
        }
        let ver = r.u8()?;
        if ver != Self::VERSION {
            return Err(format!(
                "fts5 index: unsupported version {ver} (expected {})",
                Self::VERSION
            ));
        }
        let total_docs = r.u32()?;
        let doc_count = r.u32()? as usize;
        let mut doc_lengths = HashMap::with_capacity(doc_count);
        for _ in 0..doc_count {
            let rowid = r.i64()?;
            let length = r.u32()?;
            doc_lengths.insert(rowid, length);
        }

        let term_count = r.u32()? as usize;
        let mut tokens: HashMap<String, BTreeMap<RowId, PostingList>> =
            HashMap::with_capacity(term_count);
        for _ in 0..term_count {
            let tlen = r.u32()? as usize;
            let term_bytes = r.take(tlen)?;
            let term = std::str::from_utf8(&term_bytes)
                .map_err(|e| format!("fts5 index: term not utf8: {e}"))?
                .to_string();
            let posting_count = r.u32()? as usize;
            let mut postings: BTreeMap<RowId, PostingList> = BTreeMap::new();
            for _ in 0..posting_count {
                let rowid = r.i64()?;
                let pos_count = r.u32()? as usize;
                let mut positions = Vec::with_capacity(pos_count);
                for _ in 0..pos_count {
                    positions.push(r.u32()?);
                }
                postings.insert(rowid, PostingList { positions });
            }
            tokens.insert(term, postings);
        }
        Ok(InvertedIndex {
            tokens,
            doc_lengths,
            total_docs,
        })
    }
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Result<Vec<u8>, String> {
        if self.pos + n > self.buf.len() {
            return Err("fts5 index: unexpected eof".into());
        }
        let out = self.buf[self.pos..self.pos + n].to_vec();
        self.pos += n;
        Ok(out)
    }
    fn u8(&mut self) -> Result<u8, String> {
        let b = self.take(1)?;
        Ok(b[0])
    }
    fn u32(&mut self) -> Result<u32, String> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn i64(&mut self) -> Result<i64, String> {
        let b = self.take(8)?;
        Ok(i64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vtab::fts5::tokenizer::tokenize;

    #[test]
    fn upsert_then_postings() {
        let mut idx = InvertedIndex::new();
        idx.upsert(1, &tokenize("the quick brown fox"));
        idx.upsert(2, &tokenize("lazy fox sleeps"));
        let p = idx.postings("fox");
        assert_eq!(p.len(), 2);
        assert_eq!(idx.doc_frequency("fox"), 2);
        assert_eq!(idx.doc_frequency("lazy"), 1);
        assert_eq!(idx.total_docs, 2);
    }

    #[test]
    fn remove_drops_postings() {
        let mut idx = InvertedIndex::new();
        idx.upsert(1, &tokenize("hello world"));
        idx.upsert(2, &tokenize("world peace"));
        idx.remove(1);
        let p = idx.postings("world");
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].0, 2);
        assert!(idx.postings("hello").is_empty());
        assert_eq!(idx.total_docs, 1);
    }

    #[test]
    fn upsert_replaces_previous() {
        let mut idx = InvertedIndex::new();
        idx.upsert(1, &tokenize("alpha beta"));
        idx.upsert(1, &tokenize("gamma delta"));
        assert!(idx.postings("alpha").is_empty());
        assert_eq!(idx.postings("gamma").len(), 1);
        assert_eq!(idx.total_docs, 1);
    }

    #[test]
    fn round_trip_serialization() {
        let mut idx = InvertedIndex::new();
        idx.upsert(1, &tokenize("the quick brown fox"));
        idx.upsert(2, &tokenize("the lazy dog"));
        idx.upsert(3, &tokenize("café olé"));
        let blob = idx.serialize();
        let restored = InvertedIndex::deserialize(&blob).expect("deserialize");
        assert_eq!(restored.total_docs, idx.total_docs);
        assert_eq!(restored.doc_lengths, idx.doc_lengths);
        for term in idx.tokens.keys() {
            assert_eq!(
                idx.tokens.get(term).unwrap(),
                restored.tokens.get(term).unwrap(),
            );
        }
    }

    #[test]
    fn deserialize_rejects_bad_magic() {
        let r = InvertedIndex::deserialize(b"NOPE\x01\x00\x00\x00\x00\x00\x00\x00\x00");
        assert!(r.is_err());
    }

    #[test]
    fn prefix_postings_walks_terms() {
        let mut idx = InvertedIndex::new();
        idx.upsert(1, &tokenize("quick quack quark"));
        idx.upsert(2, &tokenize("apple ant"));
        let pre = idx.prefix_postings("qu");
        let mut terms: Vec<String> = pre.into_iter().map(|(t, _)| t).collect();
        terms.sort();
        assert_eq!(terms, vec!["quack", "quark", "quick"]);
    }
}
