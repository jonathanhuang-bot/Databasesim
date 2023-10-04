use crate::page::Page;
use common::prelude::*;
use common::PAGE_SIZE;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, RwLock};

/// The struct for a heap file.  
///
/// HINT: You likely will want to design for interior mutability for concurrent accesses.
/// eg Arc<RwLock<>> on some internal members
///
/// HINT: You will probably not be able to serialize HeapFile, as it needs to maintain a link to a
/// File object, which cannot be serialized/deserialized/skipped by serde. You don't need to worry
/// about persisting read_count/write_count during serialization.
///
/// Your code should persist what information is needed to recreate the heapfile.
///
pub struct HeapFile {
    //TODO milestone hs
    pub dafile: Arc<RwLock<File>>,
    pub numpages: AtomicU16,
    pub filepath: PathBuf,
    // Track this HeapFile's container Id
    pub container_id: ContainerId,
    // The following are for profiling/ correctness checks
    pub read_count: AtomicU16,
    pub write_count: AtomicU16,
}

/// HeapFile required functions
impl HeapFile {
    /// Create a new heapfile for the given path. Return Result<Self> if able to create.
    /// Errors could arise from permissions, space, etc when trying to create the file used by HeapFile.
    pub(crate) fn new(file_path: PathBuf, container_id: ContainerId) -> Result<Self, CrustyError> {
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
        {
            Ok(f) => f,
            Err(error) => {
                return Err(CrustyError::CrustyError(format!(
                    "Cannot open or create heap file: {} {:?}",
                    file_path.to_string_lossy(),
                    error
                )))
            }
        };

        //TODO milestone hs
        let rwlock = Arc::new(RwLock::new(file));
        let atomiczero = AtomicU16::new(0);
        Ok(HeapFile {
            //TODO milestone hs
            dafile: rwlock,
            numpages: atomiczero,
            container_id,
            filepath: file_path,
            read_count: AtomicU16::new(0),
            write_count: AtomicU16::new(0),
        })
    }
    pub fn increment(&self, numpgs: u16) {
        for _i in 0..numpgs {
            self.numpages.fetch_add(1, Ordering::Relaxed);
        }
    }
    /// Return the number of pages for this HeapFile.
    /// Return type is PageId (alias for another type) as we cannot have more
    /// pages than PageId can hold.
    pub fn num_pages(&self) -> PageId {
        self.numpages.load(Ordering::Relaxed)
    }
    pub fn clone(&self) -> Self {
        Self {
            dafile: self.dafile.clone(),
            numpages: self.numpages.load(Ordering::Relaxed).into(),
            container_id: self.container_id,
            filepath: self.filepath.clone(),
            read_count: self.read_count.load(Ordering::Relaxed).into(),
            write_count: self.write_count.load(Ordering::Relaxed).into(),
        }
    }

    /// Read the page from the file.
    /// Errors could arise from the filesystem or invalid pageId
    /// Note: that std::io::{Seek, SeekFrom} require Write locks on the underlying std::fs::File
    pub(crate) fn read_page_from_file(&self, pid: PageId) -> Result<Page, CrustyError> {
        // If profiling count reads
        #[cfg(feature = "profile")]
        {
            self.read_count.fetch_add(1, Ordering::Relaxed);
        }

        // Seek to the appropriate position in the file
        let f = &mut self.dafile.write().unwrap();
        let offset = pid as u64 * PAGE_SIZE as u64;
        f.seek(SeekFrom::Start(offset))?;

        // Read in the page data
        let mut buf = [0; PAGE_SIZE];
        f.read_exact(&mut buf)?;
        f.flush()?;

        Ok(Page { data: buf })
    }

    /// Take a page and write it to the underlying file.
    /// This could be an existing page or a new page
    pub(crate) fn write_page_to_file(&self, page: Page) -> Result<(), CrustyError> {
        trace!(
            "Writing page {} to file {}",
            page.get_page_id(),
            self.container_id
        );
        //If profiling count writes
        #[cfg(feature = "profile")]
        {
            self.write_count.fetch_add(1, Ordering::Relaxed);
        }
        if page.get_page_id() == self.numpages.load(Ordering::Relaxed) {
            self.numpages.fetch_add(1, Ordering::Relaxed);
        }
        let f = &mut self.dafile.write().unwrap();
        let loc = page.get_page_id() as usize * PAGE_SIZE;
        f.seek(SeekFrom::Start(loc.try_into().unwrap())).unwrap();
        f.write_all(&page.data).unwrap();
        //drop(f);
        Ok(())
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use common::testutil::*;
    use temp_testdir::TempDir;

    #[test]
    fn hs_hf_insert() {
        init();

        //Create a temp file
        let f = gen_random_test_sm_dir();
        let tdir = TempDir::new(f, true);
        let mut f = tdir.to_path_buf();
        f.push(gen_rand_string(4));
        f.set_extension("hf");

        let mut hf = HeapFile::new(f.to_path_buf(), 0).expect("Unable to create HF for test");

        // Make a page and write
        let mut p0 = Page::new(0);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let p0_bytes = p0.to_bytes();

        hf.write_page_to_file(p0);
        //check the page
        assert_eq!(1, hf.num_pages());
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.to_bytes());

        //Add another page
        let mut p1 = Page::new(1);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let p1_bytes = p1.to_bytes();

        hf.write_page_to_file(p1);

        assert_eq!(2, hf.num_pages());
        //Recheck page0
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.to_bytes());

        //check page 1
        let checkp1 = hf.read_page_from_file(1).unwrap();
        assert_eq!(p1_bytes, checkp1.to_bytes());

        #[cfg(feature = "profile")]
        {
            assert_eq!(*hf.read_count.get_mut(), 3);
            assert_eq!(*hf.write_count.get_mut(), 2);
        }
    }
}
