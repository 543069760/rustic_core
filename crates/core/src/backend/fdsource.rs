//! A [`ReadSource`] reading backup data from a generic [`Read`]er,
//! typically a pipe/file fed by an external archiver (e.g. `tar -cpf -`).
//!
//! The stream is chunked content-defined (CDC) just like a regular file
//! backup; the archive never touches disk. Bytes passing through the reader
//! are counted in `fed` so the caller can reconcile against the archiver's
//! own accounting (e.g. tar's `--totals` output on stderr).

use std::{
    fs::File,
    io::{self, Read},
    iter::{Once, once},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::{
    RusticResult,
    error::{ErrorKind, RusticError},
};

use super::{ReadSource, ReadSourceEntry};

/// Reader that counts bytes passing through it (for `--totals` reconciliation).
pub struct CountingFdReader {
    file: File,
    fed: Arc<AtomicU64>,
}

impl Read for CountingFdReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.file.read(buf)?;
        let _ = self.fed.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }
}

/// A [`ReadSource`] backed by an already-opened [`File`] (e.g. a pipe read end).
///
/// The file is owned by the source and closed when the backup finishes or
/// the source is dropped. `entries()` consumes the file exactly once; calling
/// it a second time yields an error instead of silently dup'ing the fd.
pub struct FdSource {
    /// Logical path inside the snapshot (e.g. `user.tar`).
    path: PathBuf,
    file: Mutex<Option<File>>,
    fed: Arc<AtomicU64>,
}

impl FdSource {
    /// Creates a new `FdSource`.
    ///
    /// # Arguments
    ///
    /// * `path` - logical filename stored inside the snapshot
    /// * `file` - the opened stream to read from (e.g. `File::from_raw_fd(pipe_read_end)`),
    ///   ownership is taken here
    /// * `fed` - shared counter for bytes consumed; must equal tar's `--totals` on success
    pub fn new(path: PathBuf, file: File, fed: Arc<AtomicU64>) -> Self {
        Self {
            path,
            file: Mutex::new(Some(file)),
            fed,
        }
    }
}

impl ReadSource for FdSource {
    type Open = CountingFdReader;
    type Iter = Once<RusticResult<ReadSourceEntry<CountingFdReader>>>;

    /// The size of a pipe stream is unknown up front; progress accounting
    /// relies on the caller-provided `sizeBytes` estimate, not on this value.
    fn size(&self) -> RusticResult<Option<u64>> {
        Ok(None)
    }

    /// Returns an iterator over the source entries (single entry).
    fn entries(&self) -> Self::Iter {
        let taken = self.file.lock().unwrap().take();
        once(match taken {
            Some(file) => {
                let reader = CountingFdReader {
                    file,
                    fed: self.fed.clone(),
                };
                ReadSourceEntry::from_path(self.path.clone(), Some(reader)).map_err(|err| {
                    RusticError::with_source(
                        ErrorKind::Backend,
                        "Failed to create ReadSourceEntry from FdSource",
                        err,
                    )
                })
            }
            None => Err(RusticError::new(
                ErrorKind::Backend,
                "FdSource entries() called more than once: stream already consumed",
            )),
        })
    }
}