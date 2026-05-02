use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use rsqlite_vfs::{LockType, OpenFlags, SyncFlags, Vfs, VfsError, VfsFile};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    FileSystemDirectoryHandle, FileSystemGetFileOptions, FileSystemReadWriteOptions,
    FileSystemSyncAccessHandle,
};

pub struct OpfsVfs {
    root_dir: FileSystemDirectoryHandle,
    /// Shared handle map. `clone_box` (used by sqlite to hand the VFS to
    /// multiple owners) bumps the `Arc` rather than deep-cloning the
    /// HashMap — every JS-side `FileSystemSyncAccessHandle` then has a
    /// single owning record, and `Drop` only closes the OPFS handles
    /// when the last `OpfsVfs` reference goes away. The previous
    /// `RefCell<HashMap<...>>` shape produced "file already closed"
    /// errors: `RefCell::clone` deep-cloned the map but the JS handles
    /// inside were JsValue refs to the *same* underlying OPFS handles,
    /// so dropping the first cloned VFS closed the handles still in
    /// active use by the second.
    handles: Arc<RefCell<HashMap<String, FileSystemSyncAccessHandle>>>,
}

// `Arc<RefCell<HashMap<…JsValue…>>>` would normally be `!Send`, but
// wasm32 is single-threaded — there's no scenario where the VFS is
// shared across threads. The unsafe assert mirrors the `web_sys`
// convention for JS-handle wrappers.
unsafe impl Send for OpfsVfs {}

impl OpfsVfs {
    pub async fn new() -> Result<Self, JsValue> {
        let global: web_sys::WorkerGlobalScope = js_sys::global().unchecked_into();
        let navigator = global.navigator();
        let storage = navigator.storage();
        let dir_val: JsValue = JsFuture::from(storage.get_directory()).await?;
        let root_dir: FileSystemDirectoryHandle = dir_val.unchecked_into();
        Ok(Self {
            root_dir,
            handles: Arc::new(RefCell::new(HashMap::new())),
        })
    }

    pub async fn open_file(&self, path: &str, create: bool) -> Result<(), JsValue> {
        let opts = FileSystemGetFileOptions::new();
        opts.set_create(create);
        let file_handle: web_sys::FileSystemFileHandle =
            JsFuture::from(self.root_dir.get_file_handle_with_options(path, &opts))
                .await?
                .unchecked_into();
        let sync_handle: FileSystemSyncAccessHandle =
            JsFuture::from(file_handle.create_sync_access_handle())
                .await?
                .unchecked_into();
        self.handles
            .borrow_mut()
            .insert(path.to_string(), sync_handle);
        Ok(())
    }

    /// Pre-register `count` sharded files for the multiplex VFS. Each shard
    /// is opened with `create: true` so the SyncAccessHandle exists ahead of
    /// time — OPFS only exposes async handle creation, but sharded writes
    /// from the engine happen synchronously, so we have to hold all the
    /// handles we might need before any write begins.
    pub async fn register_shards(&self, base: &str, count: usize) -> Result<(), JsValue> {
        for i in 0..count {
            let name = format!("{}.{:03}", base, i);
            self.open_file(&name, true).await?;
        }
        Ok(())
    }

    fn get_handle(&self, path: &str) -> Option<FileSystemSyncAccessHandle> {
        self.handles.borrow().get(path).cloned()
    }
}

impl Vfs for OpfsVfs {
    fn open(&self, path: &str, _flags: OpenFlags) -> rsqlite_vfs::Result<Box<dyn VfsFile>> {
        let handle = self
            .get_handle(path)
            .ok_or_else(|| VfsError::NotFound(path.to_string()))?;
        Ok(Box::new(OpfsFile {
            handle,
            lock: LockType::None,
        }))
    }

    fn delete(&self, path: &str) -> rsqlite_vfs::Result<()> {
        if let Some(handle) = self.handles.borrow_mut().remove(path) {
            handle.close();
        }
        Ok(())
    }

    fn exists(&self, path: &str) -> rsqlite_vfs::Result<bool> {
        Ok(self.handles.borrow().contains_key(path))
    }

    fn clone_box(&self) -> Box<dyn Vfs> {
        // Share the handles map via Arc — every `OpfsVfs` issued by
        // `clone_box` references the same underlying OPFS handles, so
        // dropping one clone doesn't close the handles still being
        // used by the others. `Drop` only closes when the last VFS
        // reference goes away (`Arc::strong_count == 1`).
        Box::new(OpfsVfs {
            root_dir: self.root_dir.clone(),
            handles: Arc::clone(&self.handles),
        })
    }
}

pub struct OpfsFile {
    handle: FileSystemSyncAccessHandle,
    lock: LockType,
}

unsafe impl Send for OpfsFile {}

impl VfsFile for OpfsFile {
    fn read(&self, offset: u64, buf: &mut [u8]) -> rsqlite_vfs::Result<usize> {
        let opts = FileSystemReadWriteOptions::new();
        opts.set_at(offset as f64);
        let bytes_read = self
            .handle
            .read_with_u8_array_and_options(buf, &opts)
            .map_err(|e| VfsError::Other(format!("OPFS read: {e:?}")))?;
        Ok(bytes_read as usize)
    }

    fn write(&mut self, offset: u64, data: &[u8]) -> rsqlite_vfs::Result<()> {
        let opts = FileSystemReadWriteOptions::new();
        opts.set_at(offset as f64);
        self.handle
            .write_with_u8_array_and_options(data, &opts)
            .map_err(|e| VfsError::Other(format!("OPFS write: {e:?}")))?;
        Ok(())
    }

    fn file_size(&self) -> rsqlite_vfs::Result<u64> {
        let size = self
            .handle
            .get_size()
            .map_err(|e| VfsError::Other(format!("OPFS getSize: {e:?}")))?;
        Ok(size as u64)
    }

    fn truncate(&mut self, size: u64) -> rsqlite_vfs::Result<()> {
        self.handle
            .truncate_with_f64(size as f64)
            .map_err(|e| VfsError::Other(format!("OPFS truncate: {e:?}")))?;
        Ok(())
    }

    fn sync(&mut self, _flags: SyncFlags) -> rsqlite_vfs::Result<()> {
        self.handle
            .flush()
            .map_err(|e| VfsError::Other(format!("OPFS flush: {e:?}")))?;
        Ok(())
    }

    fn lock(&mut self, lock_type: LockType) -> rsqlite_vfs::Result<()> {
        self.lock = lock_type;
        Ok(())
    }

    fn unlock(&mut self, lock_type: LockType) -> rsqlite_vfs::Result<()> {
        self.lock = lock_type;
        Ok(())
    }
}

impl Drop for OpfsVfs {
    fn drop(&mut self) {
        // Only close the OPFS sync access handles when this is the
        // last `OpfsVfs` referencing them — otherwise we'd close
        // handles that other live `OpfsVfs` clones (via `clone_box`)
        // are still actively using, surfacing as `InvalidStateError:
        // file already closed` on subsequent VFS calls.
        if Arc::strong_count(&self.handles) == 1 {
            for handle in self.handles.borrow().values() {
                handle.close();
            }
        }
    }
}
