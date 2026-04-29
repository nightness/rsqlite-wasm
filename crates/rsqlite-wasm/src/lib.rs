use wasm_bindgen::prelude::*;

use rsqlite_core::database::Database;
use rsqlite_core::types::Value;
use rsqlite_vfs::Vfs;
use rsqlite_vfs::memory::MemoryVfs;

#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}

#[wasm_bindgen]
pub struct WasmDatabase {
    db: Database,
    vfs: MemoryVfs,
    path: String,
}

#[wasm_bindgen]
impl WasmDatabase {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Result<WasmDatabase, JsError> {
        let vfs = MemoryVfs::new();
        let db = Database::create(&vfs, "memory.db").map_err(to_js_error)?;
        Ok(WasmDatabase {
            db,
            vfs,
            path: "memory.db".to_string(),
        })
    }

    #[wasm_bindgen(js_name = "openInMemory")]
    pub fn open_in_memory() -> Result<WasmDatabase, JsError> {
        WasmDatabase::new()
    }

    pub fn exec(&mut self, sql: &str) -> Result<u64, JsError> {
        let result = self.db.execute(sql).map_err(to_js_error)?;
        Ok(result.rows_affected)
    }

    pub fn query(&mut self, sql: &str) -> Result<JsValue, JsError> {
        let result = self.db.query(sql).map_err(to_js_error)?;

        let rows = js_sys::Array::new();
        for row in &result.rows {
            let obj = js_sys::Object::new();
            for (i, col_name) in result.columns.iter().enumerate() {
                let val = row.values.get(i).unwrap_or(&Value::Null);
                let js_val = value_to_js(val);
                js_sys::Reflect::set(&obj, &JsValue::from_str(col_name), &js_val)
                    .map_err(|_| JsError::new("failed to set property"))?;
            }
            rows.push(&obj);
        }
        Ok(rows.into())
    }

    #[wasm_bindgen(js_name = "queryOne")]
    pub fn query_one(&mut self, sql: &str) -> Result<JsValue, JsError> {
        let result = self.db.query(sql).map_err(to_js_error)?;

        if result.rows.is_empty() {
            return Ok(JsValue::NULL);
        }

        let row = &result.rows[0];
        let obj = js_sys::Object::new();
        for (i, col_name) in result.columns.iter().enumerate() {
            let val = row.values.get(i).unwrap_or(&Value::Null);
            let js_val = value_to_js(val);
            js_sys::Reflect::set(&obj, &JsValue::from_str(col_name), &js_val)
                .map_err(|_| JsError::new("failed to set property"))?;
        }
        Ok(obj.into())
    }

    #[wasm_bindgen(js_name = "execMany")]
    pub fn exec_many(&mut self, sql: &str) -> Result<(), JsError> {
        let statements: Vec<&str> = sql.split(';').collect();
        for stmt in statements {
            let trimmed = stmt.trim();
            if trimmed.is_empty() {
                continue;
            }
            let full = format!("{trimmed};");
            self.db.execute_sql(&full).map_err(to_js_error)?;
        }
        Ok(())
    }

    #[wasm_bindgen(js_name = "toBuffer")]
    pub fn to_buffer(&mut self) -> Result<Vec<u8>, JsError> {
        use rsqlite_vfs::OpenFlags;

        let flags = OpenFlags {
            create: false,
            read_write: false,
            delete_on_close: false,
        };
        let file = self.vfs.open(&self.path, flags).map_err(to_js_error)?;
        let size = file.file_size().map_err(to_js_error)? as usize;
        let mut buf = vec![0u8; size];
        file.read(0, &mut buf).map_err(to_js_error)?;
        Ok(buf)
    }

    pub fn close(self) {}
}

fn value_to_js(val: &Value) -> JsValue {
    match val {
        Value::Null => JsValue::NULL,
        Value::Integer(i) => JsValue::from_f64(*i as f64),
        Value::Real(f) => JsValue::from_f64(*f),
        Value::Text(s) => JsValue::from_str(s),
        Value::Blob(b) => {
            let arr = js_sys::Uint8Array::new_with_length(b.len() as u32);
            arr.copy_from(b);
            arr.into()
        }
    }
}

fn to_js_error(e: impl std::fmt::Display) -> JsError {
    JsError::new(&e.to_string())
}
