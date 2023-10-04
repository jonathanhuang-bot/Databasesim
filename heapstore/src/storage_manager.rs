use crate::heapfile::HeapFile;
use crate::heapfileiter::HeapFileIterator;
use crate::page::Page;
use common::prelude::*;
use common::storage_trait::StorageTrait;
use common::testutil::gen_random_test_sm_dir;
use common::PAGE_SIZE;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};

/// The StorageManager struct
#[derive(Serialize, Deserialize)]
pub struct StorageManager {
    /// Path to database metadata files.
    #[serde(skip)]
    pub containers: Arc<RwLock<HashMap<ContainerId, HeapFile>>>,
    pub names: Arc<RwLock<HashMap<ContainerId, PathBuf>>>,
    // add hashmap with containerid and heapfile name
    pub storage_path: PathBuf,
    /// Indicates if this is a temp StorageManager (for testing)
    is_temp: bool,
}

/// The required functions in HeapStore's StorageManager that are specific for HeapFiles
impl StorageManager {
    /// Get a page if exists for a given container.
    pub(crate) fn get_page(
        &self,
        container_id: ContainerId,
        page_id: PageId,
        _tid: TransactionId,
        _perm: Permissions,
        _pin: bool,
    ) -> Option<Page> {
        let mut containers = self.containers.write().unwrap();
        if containers.contains_key(&container_id) {
            let dafile = containers.get_mut(&container_id).unwrap();
            let data = dafile.read_page_from_file(page_id).unwrap();
            return Some(data);
        }
        None
    }

    /// Write a page
    pub(crate) fn write_page(
        &self,
        container_id: ContainerId,
        page: Page,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> {
        let mut containers = self.containers.write().unwrap();
        if containers.contains_key(&container_id) {
            let dafile = containers.get_mut(&container_id).unwrap();
            return dafile.write_page_to_file(page);
        }
        Err(CrustyError::CrustyError(
            "Cannot write page to container:".to_string(),
        ))
    }

    /// Get the number of pages for a container
    #[allow(dead_code)]
    fn get_num_pages(&self, container_id: ContainerId) -> PageId {
        let mut containers = self.containers.write().unwrap();
        if containers.contains_key(&container_id) {
            let dafile = containers.get_mut(&container_id).unwrap();
            dafile.num_pages()
        } else {
            0
        }
    }

    /// Test utility function for counting reads and writes served by the heap file.
    /// Can return 0,0 for invalid container_ids
    #[allow(dead_code)]
    pub(crate) fn get_hf_read_write_count(&self, _container_id: ContainerId) -> (u16, u16) {
        panic!("TODO milestone hs");
    }

    /// For testing
    pub fn get_page_debug(&self, container_id: ContainerId, page_id: PageId) -> String {
        match self.get_page(
            container_id,
            page_id,
            TransactionId::new(),
            Permissions::ReadOnly,
            false,
        ) {
            Some(p) => {
                format!("{:?}", p)
            }
            None => String::new(),
        }
    }

    /// For testing
    pub fn get_page_bytes(&self, container_id: ContainerId, page_id: PageId) -> Vec<u8> {
        match self.get_page(
            container_id,
            page_id,
            TransactionId::new(),
            Permissions::ReadOnly,
            false,
        ) {
            Some(p) => p.to_bytes(),
            None => Vec::new(),
        }
    }
}

/// Implementation of storage trait
impl StorageTrait for StorageManager {
    type ValIterator = HeapFileIterator;

    /// Create a new storage manager that will use storage_path as the location to persist data
    /// (if the storage manager persists records on disk; not the case for memstore)
    /// For startup/shutdown: check the storage_path for data persisted in shutdown() that you can
    /// use to populate this instance of the SM. Otherwise create a new one.
    fn new(storage_path: PathBuf) -> Self {
        let persisted_data_file_path = storage_path.join("persisted_data");

        let map: HashMap<ContainerId, HeapFile> = HashMap::new();
        let maplock = Arc::new(RwLock::new(map));
        let names: HashMap<ContainerId, PathBuf> = HashMap::new();
        let nameslock = Arc::new(RwLock::new(names));
        #[allow(clippy::needless_late_init)]
        let persisted_data: Option<std::collections::HashMap<ContainerId, PathBuf>>;
        if persisted_data_file_path.exists() {
            let file = File::open(&persisted_data_file_path).unwrap();
            let mut reader = BufReader::new(file);
            let mut contents = String::new();
            reader.read_to_string(&mut contents).unwrap();
            persisted_data = serde_json::from_str(&contents).unwrap();
        } else {
            persisted_data = None;
        }

        if let Some(persisted_data) = persisted_data {
            for (container_id, path) in persisted_data.iter() {
                let bytepagelen = fs::metadata(path).unwrap().len();
                let numpgs = (bytepagelen as u16 + PAGE_SIZE as u16 - 1) / PAGE_SIZE as u16;
                let heapfile = HeapFile::new(path.clone(), *container_id).unwrap();
                heapfile.increment(numpgs);
                maplock.write().unwrap().insert(*container_id, heapfile);
                nameslock
                    .write()
                    .unwrap()
                    .insert(*container_id, path.clone());
            }
        }
        StorageManager {
            containers: maplock,
            names: (nameslock),
            storage_path: (storage_path),
            is_temp: (false),
        }
    }

    /// Create a new storage manager for testing. There is no startup/shutdown logic here: it
    /// should simply create a fresh SM and set is_temp to true
    fn new_test_sm() -> Self {
        let storage_path = gen_random_test_sm_dir();
        let map: HashMap<ContainerId, HeapFile> = HashMap::new();
        let names: HashMap<ContainerId, PathBuf> = HashMap::new();
        let is_temp = true;
        let maplock = Arc::new(RwLock::new(map));
        let nameslock = Arc::new(RwLock::new(names));
        StorageManager {
            containers: maplock,
            names: nameslock,
            storage_path,
            is_temp,
        }
    }

    /// Insert some bytes into a container for a particular value (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns the value id associated with the stored value.
    /// Function will need to find the first page that can hold the value.
    /// A new page may need to be created if no space on existing pages can be found.
    fn insert_value(
        &self,
        container_id: ContainerId,
        value: Vec<u8>,
        _tid: TransactionId,
    ) -> ValueId {
        if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        }
        let mut containers = self.containers.write().unwrap();
        let pfile = containers.get_mut(&container_id).unwrap();

        let newpfile = pfile.clone();
        let numpages = pfile.numpages.load(Ordering::Relaxed);
        let slotsize = 6;
        
        for i in 0..numpages {
            
            let mut page = pfile.read_page_from_file(i).unwrap();
            
            if page.get_free_space() > value.len() + slotsize {
              
                let slotid = page.add_value(&value).unwrap();
                newpfile.write_page_to_file(page).unwrap();
                containers.insert(container_id, newpfile);
                let val = ValueId {
                    container_id,
                    segment_id: None,
                    page_id: Some(i),
                    slot_id: Some(slotid),
                };
                return val;
            } else {
            }
        }
        
        let mut newpg = Page::new(numpages);
        let slotid = newpg.add_value(&value).unwrap();
        newpfile.write_page_to_file(newpg).unwrap();
        containers.insert(container_id, newpfile);
        ValueId {
            container_id,
            segment_id: None,
            page_id: Some(numpages),
            slot_id: Some(slotid),
        }
        //update hashma
    }

    /// Insert some bytes into a container for vector of values (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns a vector of value ids associated with the stored values.
    fn insert_values(
        &self,
        container_id: ContainerId,
        values: Vec<Vec<u8>>,
        tid: TransactionId,
    ) -> Vec<ValueId> {
        let mut ret = Vec::new();
        for v in values {
            ret.push(self.insert_value(container_id, v, tid));
        }
        ret
    }

    /// Delete the data for a value. If the valueID is not found it returns Ok() still.
    fn delete_value(&self, id: ValueId, _tid: TransactionId) -> Result<(), CrustyError> {
        let page_id = id.page_id;
        let slot_id = id.slot_id;
        if page_id.is_none() || slot_id.is_none() {
            return Ok(());
        }
        let container_id = id.container_id;
        let mut containers = self.containers.write().unwrap();
        //if container id is valid
        if containers.contains_key(&container_id) {
            //get heapfile
            let pfile = containers.get_mut(&container_id).unwrap();
            let numpages = pfile.numpages.load(Ordering::Relaxed);
            //check if the page id is less than the number of pages of this heapfile
            if page_id.unwrap() < numpages {
                //get page and delete the value
                let mut page = pfile.read_page_from_file(page_id.unwrap()).unwrap();
                let p = page.delete_value(slot_id.unwrap());
                //update heapfile with new page
                pfile.write_page_to_file(page).unwrap();
                //update hashmap with new heapfile
                let newpfile = pfile.clone();
                containers.insert(container_id, newpfile);
                match p {
                    Some(()) => Ok(()),
                    None => Err(CrustyError::CrustyError(
                        "Cannot open or create heap file:".to_string(),
                    )),
                }
            } else {
                Err(CrustyError::CrustyError(
                    "Cannot open or create heap file:".to_string(),
                ))
            }
        } else {
            Err(CrustyError::CrustyError(
                "Cannot open or create heap file:".to_string(),
            ))
        }
    }

    /// Updates a value. Returns valueID on update (which may have changed). Error on failure
    /// Any process that needs to determine if a value changed will need to compare the return valueId against
    /// the sent value.
    fn update_value(
        &self,
        value: Vec<u8>,
        id: ValueId,
        _tid: TransactionId,
    ) -> Result<ValueId, CrustyError> {
        let p = self.delete_value(id, _tid);
        match p {
            Ok(()) => Ok(self.insert_value(id.container_id, value, _tid)),
            Err(error) => Err(error),
        }
    }

    /// Create a new container to be stored.
    /// fn create_container(&self, name: String) -> ContainerId;
    /// Creates a new container object.
    /// For this milestone you will not need to utilize
    /// the container_config, name, container_type, or dependencies
    ///
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to add delta to.
    fn create_container(
        &self,
        container_id: ContainerId,
        _name: Option<String>,
        _container_type: common::ids::StateType,
        _dependencies: Option<Vec<ContainerId>>,
    ) -> Result<(), CrustyError> {
        //what is path buf
        //check if containerid exists in hashmap
        let mut containers = self.containers.write().unwrap();
        let mut names = self.names.write().unwrap();
        match containers.contains_key(&container_id) {
            //otherwise create a new container/heapfile and pathbuf and update hashmaps
            false => {
                let str = format!("{}{}", "container", container_id);
                let dirname: &Path = Path::new(&str);
                let dirpath = self.storage_path.join(dirname);
                fs::create_dir_all(&dirpath)?;
                let file_name = format!("{}.heapfile", container_id);
                let file_path = dirpath.join(file_name);
                let new_heap = HeapFile::new(file_path.clone(), container_id)?;
                containers.insert(container_id, new_heap);
                names.insert(container_id, file_path);
                Ok(())
            }
            true => Err(CrustyError::CrustyError(
                "Container already exists".to_string(),
            )),
        }
    }

    /// A wrapper function to call create container
    fn create_table(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        self.create_container(container_id, None, common::ids::StateType::BaseTable, None)
    }

    /// Remove the container and all stored values in the container.
    /// If the container is persisted remove the underlying files
    fn remove_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        let mut containers = self.containers.write().unwrap();
        let p = containers.remove(&container_id);
        match p {
            Some(_) => Ok(()),
            None => Err(CrustyError::CrustyError(
                "Cannot open or create heap file:".to_string(),
            )),
        }
    }

    /// Get an iterator that returns all valid records
    fn get_iterator(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
    ) -> Self::ValIterator {
        let mut containers = self.containers.write().unwrap();
        let hf = containers.get_mut(&container_id).unwrap();
        let p = Arc::new(hf.clone());
        HeapFileIterator::new(tid, p)
    }

    /// Get the data for a particular ValueId. Error if does not exists
    fn get_value(
        &self,
        id: ValueId,
        _tid: TransactionId,
        _perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        let mut containers = self.containers.write().unwrap();
        let containerid = id.container_id;
        let pageid = id.page_id.unwrap();
        let slotid = id.slot_id.unwrap();
        if containers.contains_key(&containerid) {
            let pfile = containers.get_mut(&containerid).unwrap();
            let page = pfile.read_page_from_file(pageid).unwrap();
            let res = page.get_value(slotid);
            match res {
                Some(t) => Ok(t),
                None => Err(CrustyError::CrustyError(
                    "Cannot open or create heap file:".to_string(),
                )),
            }
        } else {
            Err(CrustyError::CrustyError(
                "Cannot open or create heap file:".to_string(),
            ))
        }
    }

    /// Notify the storage manager that the transaction is finished so that any held resources can be released.
    fn transaction_finished(&self, _tid: TransactionId) {
        panic!("TODO milestone tm");
    }

    /// Testing utility to reset all state associated the storage manager. Deletes all data in
    /// storage path (keeping storage path as a directory). Doesn't need to serialize any data to
    /// disk as its just meant to clear state.
    ///
    /// Clear any data structures in the SM you add
    fn reset(&self) -> Result<(), CrustyError> {
        fs::remove_dir_all(self.storage_path.clone())?;
        fs::create_dir_all(self.storage_path.clone()).unwrap();
        for entry in fs::read_dir(self.storage_path.clone())? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                fs::remove_dir_all(entry.path())?;
            } else {
                fs::remove_file(entry.path())?;
            }
        }

        fs::create_dir_all(self.storage_path.clone())?;
        self.containers.write().unwrap().clear();
        self.names.write().unwrap().clear();

        Ok(())
    }

    /// If there is a buffer pool or cache it should be cleared/reset.
    /// Otherwise do nothing.
    fn clear_cache(&self) {}

    /// Shutdown the storage manager. Should be safe to call multiple times. You can assume this
    /// function will never be called on a temp SM.
    /// This should serialize the mapping between containerID and Heapfile to disk in a way that
    /// can be read by StorageManager::new.
    /// HINT: Heapfile won't be serializable/deserializable. You'll want to serialize information
    /// that can be used to create a HeapFile object pointing to the same data. You don't need to
    /// worry about recreating read_count or write_count.
    fn shutdown(&self) {
        let persisted_data_file_path = self.storage_path.join("persisted_data");

        let mut persisted_data = std::collections::HashMap::new();
        let binding = self.containers.read().unwrap();
        for (container_id, heap_file) in binding.iter() {
            //hashmap of container id and heapfile name
            persisted_data.insert(container_id, heap_file.filepath.clone());
        }

        let file = File::create(persisted_data_file_path).unwrap();
        let mut writer = BufWriter::new(file);
        let serialized = serde_json::to_string(&persisted_data).unwrap();
        writer.write_all(serialized.as_bytes()).unwrap();
    }

    fn import_csv(
        &self,
        table: &Table,
        path: String,
        _tid: TransactionId,
        container_id: ContainerId,
    ) -> Result<(), CrustyError> {
        // Err(CrustyError::CrustyError(String::from("TODO")))
        // Convert path into an absolute path.
        let path = fs::canonicalize(path)?;
        debug!("server::csv_utils trying to open file, path: {:?}", path);
        let file = fs::File::open(path)?;
        // Create csv reader.
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(file);

        // Iterate through csv records.
        let mut inserted_records = 0;
        for result in rdr.records() {
            #[allow(clippy::single_match)]
            match result {
                Ok(rec) => {
                    // Build tuple and infer types from schema.
                    let mut tuple = Tuple::new(Vec::new());
                    for (field, attr) in rec.iter().zip(table.schema.attributes()) {
                        // TODO: Type mismatch between attributes and record data>
                        match &attr.dtype() {
                            DataType::Int => {
                                let value: i32 = field.parse::<i32>().unwrap();
                                tuple.field_vals.push(Field::IntField(value));
                            }
                            DataType::String => {
                                let value: String = field.to_string().clone();
                                tuple.field_vals.push(Field::StringField(value));
                            }
                        }
                    }
                    //TODO: How should individual row insertion errors be handled?
                    debug!(
                        "server::csv_utils about to insert tuple into container_id: {:?}",
                        &container_id
                    );
                    self.insert_value(container_id, tuple.to_bytes(), _tid);
                    inserted_records += 1;
                }
                _ => {
                    // FIXME: get error from csv reader
                    error!("Could not read row from CSV");
                    return Err(CrustyError::IOError(
                        "Could not read row from CSV".to_string(),
                    ));
                }
            }
        }
        info!("Num records imported: {:?}", inserted_records);
        Ok(())
    }
}

/// Trait Impl for Drop
impl Drop for StorageManager {
    // if temp SM this clears the storage path entirely when it leaves scope; used for testing
    fn drop(&mut self) {
        if self.is_temp {
            debug!("Removing storage path on drop {:?}", self.storage_path);
            let remove_all = fs::remove_dir_all(self.storage_path.clone());
            if let Err(e) = remove_all {
                println!("Error on removing temp dir {}", e);
            }
        }
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use crate::storage_manager::StorageManager;
    use common::storage_trait::StorageTrait;
    use common::testutil::*;

    #[test]
    fn hs_sm_a_insert() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);

        let bytes = get_random_byte_vec(40);
        let tid = TransactionId::new();

        let val1 = sm.insert_value(cid, bytes.clone(), tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val1.page_id.unwrap());
        assert_eq!(0, val1.slot_id.unwrap());

        let p1 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();
        let val2 = sm.insert_value(cid, bytes, tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val2.page_id.unwrap());
        assert_eq!(1, val2.slot_id.unwrap());

        let p2 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();
        assert_ne!(p1.to_bytes()[..], p2.to_bytes()[..]);
    }

    #[test]
    fn hs_sm_b_iter_small() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);
        let tid = TransactionId::new();
        //Test one page
        let mut byte_vec: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];
        for val in &byte_vec {
            sm.insert_value(cid, val.clone(), tid);
        }
        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }

        // Should be on two pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }

        // Should be on 3 pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(300),
            get_random_byte_vec(500),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }
    }

    #[test]
    #[ignore]
    fn hs_sm_b_iter_large() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        //.unwrap()
        sm.create_table(cid);
        let tid = TransactionId::new();

        let vals = get_random_vec_of_byte_vec(1000, 40, 400);
        sm.insert_values(cid, vals, tid);
        let mut count = 0;
        for _ in sm.get_iterator(cid, tid, Permissions::ReadOnly) {
            count += 1;
        }
        assert_eq!(1000, count);
    }
}
