use crate::codec::{Record, Value};
use crate::error::{Result, StorageError};
use crate::header::HEADER_SIZE;
use crate::pager::Pager;
use crate::varint;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    InteriorIndex = 0x02,
    InteriorTable = 0x05,
    LeafIndex = 0x0A,
    LeafTable = 0x0D,
}

impl PageType {
    fn from_u8(v: u8) -> Result<Self> {
        match v {
            0x02 => Ok(Self::InteriorIndex),
            0x05 => Ok(Self::InteriorTable),
            0x0A => Ok(Self::LeafIndex),
            0x0D => Ok(Self::LeafTable),
            _ => Err(StorageError::Corrupt(format!(
                "invalid B-tree page type: {v:#04x}"
            ))),
        }
    }

    pub fn is_leaf(self) -> bool {
        matches!(self, Self::LeafTable | Self::LeafIndex)
    }

    pub fn is_table(self) -> bool {
        matches!(self, Self::InteriorTable | Self::LeafTable)
    }
}

#[derive(Debug)]
pub struct BTreePageHeader {
    pub page_type: PageType,
    pub first_freeblock: u16,
    pub cell_count: u16,
    pub cell_content_offset: u32,
    pub fragmented_free_bytes: u8,
    pub right_most_pointer: Option<u32>,
}

impl BTreePageHeader {
    pub fn header_size(&self) -> usize {
        if self.page_type.is_leaf() {
            8
        } else {
            12
        }
    }
}

/// Parse the B-tree page header from raw page data.
/// `offset` is where the B-tree header starts (100 for page 1, 0 for others).
pub fn parse_btree_header(data: &[u8], offset: usize) -> Result<BTreePageHeader> {
    let page_type = PageType::from_u8(data[offset])?;
    let first_freeblock = u16::from_be_bytes([data[offset + 1], data[offset + 2]]);
    let cell_count = u16::from_be_bytes([data[offset + 3], data[offset + 4]]);
    let raw_cell_content = u16::from_be_bytes([data[offset + 5], data[offset + 6]]);
    let cell_content_offset = if raw_cell_content == 0 {
        65536u32
    } else {
        raw_cell_content as u32
    };
    let fragmented_free_bytes = data[offset + 7];

    let right_most_pointer = if !page_type.is_leaf() {
        Some(u32::from_be_bytes([
            data[offset + 8],
            data[offset + 9],
            data[offset + 10],
            data[offset + 11],
        ]))
    } else {
        None
    };

    Ok(BTreePageHeader {
        page_type,
        first_freeblock,
        cell_count,
        cell_content_offset,
        fragmented_free_bytes,
        right_most_pointer,
    })
}

/// Read the cell pointer array from a B-tree page.
fn read_cell_pointers(data: &[u8], offset: usize, count: u16) -> Vec<u16> {
    let mut pointers = Vec::with_capacity(count as usize);
    for i in 0..count as usize {
        let pos = offset + i * 2;
        let ptr = u16::from_be_bytes([data[pos], data[pos + 1]]);
        pointers.push(ptr);
    }
    pointers
}

/// Parsed cell from a leaf table B-tree page.
#[derive(Debug)]
pub struct TableLeafCell {
    pub rowid: i64,
    pub payload: Vec<u8>,
}

/// Parsed cell from an interior table B-tree page.
#[derive(Debug)]
pub struct TableInteriorCell {
    pub left_child_page: u32,
    pub rowid: i64,
}

fn parse_table_leaf_cell(data: &[u8], offset: usize, usable_size: u32) -> Result<TableLeafCell> {
    let (payload_size, n1) = varint::read_varint(&data[offset..]);
    let (rowid, n2) = varint::read_varint(&data[offset + n1..]);
    let payload_start = offset + n1 + n2;
    let payload_size = payload_size as usize;

    // Check for overflow
    let max_local = max_local_payload_leaf(usable_size) as usize;
    let local_size = if payload_size <= max_local {
        payload_size
    } else {
        let min_local = min_local_payload(usable_size) as usize;
        let mut local = min_local + (payload_size - min_local) % (usable_size as usize - 4);
        if local > max_local {
            local = min_local;
        }
        local
    };

    if local_size == payload_size {
        // No overflow
        let payload = data[payload_start..payload_start + payload_size].to_vec();
        Ok(TableLeafCell {
            rowid: rowid as i64,
            payload,
        })
    } else {
        // Has overflow — for now, just read the local part
        // TODO: follow overflow page chain
        let payload = data[payload_start..payload_start + local_size].to_vec();
        Ok(TableLeafCell {
            rowid: rowid as i64,
            payload,
        })
    }
}

fn parse_table_interior_cell(data: &[u8], offset: usize) -> TableInteriorCell {
    let left_child = u32::from_be_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ]);
    let (rowid, _) = varint::read_varint(&data[offset + 4..]);
    TableInteriorCell {
        left_child_page: left_child,
        rowid: rowid as i64,
    }
}

fn max_local_payload_leaf(usable_size: u32) -> u32 {
    usable_size - 35
}

fn min_local_payload(usable_size: u32) -> u32 {
    (usable_size - 12) * 32 / 255 - 23
}

/// A cursor for traversing a table B-tree (keyed by rowid).
pub struct BTreeCursor<'a> {
    pager: &'a mut Pager,
    root_page: u32,
    /// Stack of (page_number, child_index) representing current position.
    /// For interior pages, child_index tracks which child subtree we've visited:
    ///   0..cell_count-1 = left child of cell[i], cell_count = rightmost pointer.
    /// For leaf pages, child_index is the cell index.
    stack: Vec<(u32, usize)>,
    state: CursorState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CursorState {
    Invalid,
    Valid,
    AtEnd,
}

/// A row returned by the cursor.
#[derive(Debug)]
pub struct CursorRow {
    pub rowid: i64,
    pub record: Record,
}

impl<'a> BTreeCursor<'a> {
    pub fn new(pager: &'a mut Pager, root_page: u32) -> Self {
        Self {
            pager,
            root_page,
            stack: Vec::new(),
            state: CursorState::Invalid,
        }
    }

    /// Move to the first row in the table.
    pub fn first(&mut self) -> Result<bool> {
        self.stack.clear();
        self.state = CursorState::Invalid;
        self.descend_to_leftmost(self.root_page)?;
        self.check_valid()
    }

    /// Move to the next row. Returns false if at end.
    pub fn next(&mut self) -> Result<bool> {
        if self.state != CursorState::Valid {
            return Ok(false);
        }

        // Advance the leaf cell index
        if let Some(entry) = self.stack.last_mut() {
            entry.1 += 1;
        }

        loop {
            let (page_num, idx) = match self.stack.last().copied() {
                Some(entry) => entry,
                None => {
                    self.state = CursorState::AtEnd;
                    return Ok(false);
                }
            };

            let page = self.pager.get_page(page_num)?.data.clone();
            let offset = btree_header_offset(page_num);
            let header = parse_btree_header(&page, offset)?;

            if header.page_type.is_leaf() {
                if idx < header.cell_count as usize {
                    self.state = CursorState::Valid;
                    return Ok(true);
                }
                // Leaf exhausted, pop and advance parent
                self.stack.pop();
                if let Some(entry) = self.stack.last_mut() {
                    entry.1 += 1;
                }
            } else {
                // Interior page: idx is the child we need to visit next.
                // Children are: cell[0].left, cell[1].left, ..., cell[N-1].left, rightmost
                let total_children = header.cell_count as usize + 1;

                if idx < total_children {
                    let child_page = self.get_child_page(&page, offset, &header, idx)?;
                    self.descend_to_leftmost(child_page)?;
                    return self.check_valid();
                }
                // All children visited, pop and advance parent
                self.stack.pop();
                if let Some(entry) = self.stack.last_mut() {
                    entry.1 += 1;
                }
            }
        }
    }

    /// Get the current row (rowid + decoded record).
    pub fn current(&mut self) -> Result<CursorRow> {
        if self.state != CursorState::Valid {
            return Err(StorageError::Other("cursor not positioned".to_string()));
        }

        let &(page_num, cell_idx) = self.stack.last().unwrap();
        let page = self.pager.get_page(page_num)?.data.clone();
        let offset = btree_header_offset(page_num);
        let header = parse_btree_header(&page, offset)?;
        let usable = self.pager.usable_size();

        let pointers =
            read_cell_pointers(&page, offset + header.header_size(), header.cell_count);
        let cell_offset = pointers[cell_idx] as usize;
        let cell = parse_table_leaf_cell(&page, cell_offset, usable)?;

        let record = Record::decode(&cell.payload)?;
        Ok(CursorRow {
            rowid: cell.rowid,
            record,
        })
    }

    /// Collect all rows from the cursor (resets to first).
    pub fn collect_all(&mut self) -> Result<Vec<CursorRow>> {
        let mut rows = Vec::new();
        let mut has_row = self.first()?;
        while has_row {
            rows.push(self.current()?);
            has_row = self.next()?;
        }
        Ok(rows)
    }

    fn get_child_page(
        &mut self,
        page_data: &[u8],
        offset: usize,
        header: &BTreePageHeader,
        child_idx: usize,
    ) -> Result<u32> {
        let cell_count = header.cell_count as usize;
        if child_idx < cell_count {
            let pointers =
                read_cell_pointers(page_data, offset + header.header_size(), header.cell_count);
            let cell_offset = pointers[child_idx] as usize;
            let cell = parse_table_interior_cell(page_data, cell_offset);
            Ok(cell.left_child_page)
        } else {
            header.right_most_pointer.ok_or_else(|| {
                StorageError::Corrupt("interior page missing rightmost pointer".to_string())
            })
        }
    }

    fn descend_to_leftmost(&mut self, page_num: u32) -> Result<()> {
        let mut current_page = page_num;
        loop {
            let page_data = self.pager.get_page(current_page)?.data.clone();
            let offset = btree_header_offset(current_page);
            let header = parse_btree_header(&page_data, offset)?;

            if header.page_type.is_leaf() {
                self.stack.push((current_page, 0));
                return Ok(());
            }

            // Interior page: push with child_idx=0 (first child = left pointer of cell[0])
            self.stack.push((current_page, 0));

            let child = self.get_child_page(&page_data, offset, &header, 0)?;
            current_page = child;
        }
    }

    fn check_valid(&mut self) -> Result<bool> {
        if let Some(&(page_num, cell_idx)) = self.stack.last() {
            let page_data = self.pager.get_page(page_num)?.data.clone();
            let offset = btree_header_offset(page_num);
            let header = parse_btree_header(&page_data, offset)?;
            if header.page_type.is_leaf() && cell_idx < header.cell_count as usize {
                self.state = CursorState::Valid;
                return Ok(true);
            }
        }
        self.state = CursorState::AtEnd;
        Ok(false)
    }
}

fn btree_header_offset(page_num: u32) -> usize {
    if page_num == 1 {
        HEADER_SIZE
    } else {
        0
    }
}

/// Read all rows from the sqlite_schema table (page 1).
pub fn read_schema(pager: &mut Pager) -> Result<Vec<SchemaEntry>> {
    let mut cursor = BTreeCursor::new(pager, 1);
    let rows = cursor.collect_all()?;

    let mut entries = Vec::new();
    for row in rows {
        if row.record.values.len() < 5 {
            continue;
        }
        let entry_type = match &row.record.values[0] {
            Value::Text(s) => s.clone(),
            _ => continue,
        };
        let name = match &row.record.values[1] {
            Value::Text(s) => s.clone(),
            _ => continue,
        };
        let tbl_name = match &row.record.values[2] {
            Value::Text(s) => s.clone(),
            _ => continue,
        };
        let rootpage = match &row.record.values[3] {
            Value::Integer(n) => *n as u32,
            _ => 0,
        };
        let sql = match &row.record.values[4] {
            Value::Text(s) => Some(s.clone()),
            Value::Null => None,
            _ => None,
        };

        entries.push(SchemaEntry {
            entry_type,
            name,
            tbl_name,
            rootpage,
            sql,
        });
    }

    Ok(entries)
}

#[derive(Debug, Clone)]
pub struct SchemaEntry {
    pub entry_type: String,
    pub name: String,
    pub tbl_name: String,
    pub rootpage: u32,
    pub sql: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsqlite_vfs::memory::MemoryVfs;

    #[test]
    fn parse_empty_leaf_page() {
        let vfs = MemoryVfs::new();
        let mut pager = Pager::create(&vfs, "test.db").unwrap();
        let page = pager.get_page(1).unwrap();

        let header = parse_btree_header(&page.data, HEADER_SIZE).unwrap();
        assert_eq!(header.page_type, PageType::LeafTable);
        assert_eq!(header.cell_count, 0);
        assert!(header.right_most_pointer.is_none());
    }

    #[test]
    fn cursor_on_empty_table() {
        let vfs = MemoryVfs::new();
        let mut pager = Pager::create(&vfs, "test.db").unwrap();
        let mut cursor = BTreeCursor::new(&mut pager, 1);
        assert!(!cursor.first().unwrap());
    }

    #[test]
    fn read_real_sqlite_database() {
        let test_db = "/tmp/rsqlite_btree_test.db";
        let _ = std::fs::remove_file(test_db);
        let status = std::process::Command::new("sqlite3")
            .arg(test_db)
            .arg(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);\
                 INSERT INTO users VALUES (1, 'Alice', 30);\
                 INSERT INTO users VALUES (2, 'Bob', 25);\
                 INSERT INTO users VALUES (3, 'Charlie', 35);",
            )
            .status();

        match status {
            Ok(s) if s.success() => {
                let vfs = rsqlite_vfs::native::NativeVfs::new();
                let mut pager = Pager::open(&vfs, test_db).unwrap();

                // Read schema
                let schema = read_schema(&mut pager).unwrap();
                assert!(
                    !schema.is_empty(),
                    "schema should have at least one entry"
                );
                let table_entry = schema.iter().find(|e| e.name == "users").unwrap();
                assert_eq!(table_entry.entry_type, "table");
                assert_eq!(table_entry.tbl_name, "users");
                assert!(table_entry.rootpage > 0);

                // Read table data
                let root = table_entry.rootpage;
                let mut cursor = BTreeCursor::new(&mut pager, root);
                let rows = cursor.collect_all().unwrap();
                assert_eq!(rows.len(), 3);

                // SQLite stores all columns in the record, including the INTEGER PRIMARY KEY
                // (which is NULL in the record since the rowid carries the value).
                // So: [NULL or rowid, name, age]
                assert_eq!(rows[0].rowid, 1);
                let vals = &rows[0].record.values;
                // Find name and age — they are the last two values
                let name_idx = vals.len() - 2;
                let age_idx = vals.len() - 1;
                assert_eq!(vals[name_idx], Value::Text("Alice".to_string()));
                assert_eq!(vals[age_idx], Value::Integer(30));

                assert_eq!(rows[1].rowid, 2);
                assert_eq!(
                    rows[1].record.values[name_idx],
                    Value::Text("Bob".to_string())
                );
                assert_eq!(rows[1].record.values[age_idx], Value::Integer(25));

                assert_eq!(rows[2].rowid, 3);
                assert_eq!(
                    rows[2].record.values[name_idx],
                    Value::Text("Charlie".to_string())
                );
                assert_eq!(rows[2].record.values[age_idx], Value::Integer(35));

                let _ = std::fs::remove_file(test_db);
            }
            _ => {
                eprintln!("sqlite3 not available, skipping real database test");
            }
        }
    }

    #[test]
    fn read_schema_from_real_db() {
        let test_db = "/tmp/rsqlite_schema_test.db";
        let _ = std::fs::remove_file(test_db);
        let status = std::process::Command::new("sqlite3")
            .arg(test_db)
            .arg(
                "CREATE TABLE t1 (a INTEGER, b TEXT);\
                 CREATE TABLE t2 (x REAL, y BLOB);\
                 CREATE INDEX idx_t1_a ON t1(a);",
            )
            .status();

        match status {
            Ok(s) if s.success() => {
                let vfs = rsqlite_vfs::native::NativeVfs::new();
                let mut pager = Pager::open(&vfs, test_db).unwrap();
                let schema = read_schema(&mut pager).unwrap();

                let tables: Vec<_> = schema
                    .iter()
                    .filter(|e| e.entry_type == "table")
                    .collect();
                let indexes: Vec<_> = schema
                    .iter()
                    .filter(|e| e.entry_type == "index")
                    .collect();

                assert_eq!(tables.len(), 2);
                assert!(tables.iter().any(|t| t.name == "t1"));
                assert!(tables.iter().any(|t| t.name == "t2"));

                assert_eq!(indexes.len(), 1);
                assert_eq!(indexes[0].name, "idx_t1_a");
                assert_eq!(indexes[0].tbl_name, "t1");

                let _ = std::fs::remove_file(test_db);
            }
            _ => {
                eprintln!("sqlite3 not available, skipping schema test");
            }
        }
    }

    #[test]
    fn read_larger_database() {
        let test_db = "/tmp/rsqlite_larger_test.db";
        let _ = std::fs::remove_file(test_db);

        // Insert enough rows to potentially span multiple pages
        let mut sql = String::from("CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT);");
        for i in 1..=200 {
            sql.push_str(&format!(
                "INSERT INTO data VALUES ({i}, 'value_{i}_padding_to_make_it_longer_{i}');"
            ));
        }

        let status = std::process::Command::new("sqlite3")
            .arg(test_db)
            .arg(&sql)
            .status();

        match status {
            Ok(s) if s.success() => {
                let vfs = rsqlite_vfs::native::NativeVfs::new();
                let mut pager = Pager::open(&vfs, test_db).unwrap();

                let schema = read_schema(&mut pager).unwrap();
                let table_entry = schema.iter().find(|e| e.name == "data").unwrap();

                let mut cursor = BTreeCursor::new(&mut pager, table_entry.rootpage);
                let rows = cursor.collect_all().unwrap();
                assert_eq!(rows.len(), 200, "should have 200 rows");

                // Verify ordering (should be by rowid)
                for (i, row) in rows.iter().enumerate() {
                    assert_eq!(row.rowid, (i + 1) as i64);
                }

                // Verify last row content (value column is the last in the record)
                let last = &rows[199];
                assert_eq!(last.rowid, 200);
                let last_val = last.record.values.last().unwrap();
                if let Value::Text(s) = last_val {
                    assert!(s.contains("value_200"));
                } else {
                    panic!("expected text value, got {last_val:?}");
                }

                let _ = std::fs::remove_file(test_db);
            }
            _ => {
                eprintln!("sqlite3 not available, skipping larger database test");
            }
        }
    }
}
