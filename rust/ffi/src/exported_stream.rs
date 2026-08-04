// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Export a [`RecordBatchReader`] as an [`FFI_ArrowArrayStream`] with full ADBC error
//! reporting.
//!
//! The Arrow C stream interface can only report an errno-compatible error code and a message
//! from its callbacks, while ADBC 1.1.0 defines a richer error model (status code, SQLSTATE,
//! vendor code and key/value details) and the `AdbcErrorFromArrayStream` API to recover it
//! from a stream returned by a driver.
//!
//! `arrow-rs`'s own exporter ([`FFI_ArrowArrayStream::new`]) cannot support this: it stashes
//! only the error message, and it maps every error to one of `ENOSYS`/`ENOMEM`/`EIO`/`EINVAL`
//! based solely on the [`ArrowError`] variant. Rust ADBC drivers surface their rich
//! [`adbc_core::error::Error`] through [`ArrowError::ExternalError`], which lands in the
//! catch-all `EINVAL` — so not even a cancelled query could report `ECANCELED`.
//!
//! This module is the same export machinery, adapted from `arrow-rs`'s `ffi_stream.rs`
//! (Apache-2.0), except that on failure the [`adbc_core::error::Error`] carried by the error's
//! [source chain](std::error::Error::source) (or one synthesized from the [`ArrowError`]
//! variant when the chain carries none) is stashed in the stream's private data, and:
//!
//! * `get_schema`/`get_next` return the errno for the error's [`Status`], through a direct
//!   port of the C driver framework's canonical `InternalAdbcStatusCodeToErrno` table
//!   (`c/driver/common/utils.c`) — the same table the in-tree C/C++ drivers (which implement
//!   their stream callbacks by hand, e.g. the PostgreSQL driver's `TupleReader`) return from
//!   `get_next`. In particular a cancelled operation reports `ECANCELED`, as the C++
//!   validation suite's `SqlQueryCancel` expects after `AdbcStatementCancel`.
//! * [`error_from_array_stream`] — wired into the exported driver table as the ADBC 1.1.0
//!   `ErrorFromArrayStream` function — hands the consumer the stashed error as an
//!   [`FFI_AdbcError`] (message, SQLSTATE, vendor code and details) together with its
//!   [`AdbcStatusCode`], with none of the lossy errno projection.

use std::collections::BTreeSet;
use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr::addr_of;
use std::sync::Mutex;

use adbc_core::constants::ADBC_STATUS_OK;
use adbc_core::error::{AdbcStatusCode, Error, Status};
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{Array, RecordBatchReader, StructArray};
use arrow_schema::ArrowError;
use libc::{EACCES, ECANCELED, EEXIST, EINVAL, EIO, ENOENT, ENOTSUP, ETIMEDOUT};

use crate::FFI_AdbcError;

/// Export `reader` as an [`FFI_ArrowArrayStream`] that retains the rich ADBC error behind any
/// failure for retrieval through [`error_from_array_stream`], and whose callbacks report the
/// errno for the error's ADBC status.
pub(crate) fn export_reader(reader: Box<dyn RecordBatchReader + Send>) -> FFI_ArrowArrayStream {
    let private_data = Box::new(StreamPrivateData {
        reader,
        last_message: None,
        last_status: ADBC_STATUS_OK,
        last_error: FFI_AdbcError::default(),
    });
    let private_data = Box::into_raw(private_data);
    exported_streams()
        .expect("Poisoned exported-streams registry")
        .insert(private_data as usize);
    FFI_ArrowArrayStream {
        get_schema: Some(get_schema),
        get_next: Some(get_next),
        get_last_error: Some(get_last_error),
        release: Some(release_stream),
        private_data: private_data as *mut c_void,
    }
}

/// The private data of every live stream created by [`export_reader`], so that
/// [`error_from_array_stream`] can recognize this exporter's streams.
///
/// The C/C++ drivers recognize their own streams by comparing the stream's release callback
/// against their own by address, but that technique is not sound in Rust: pointers to the same
/// function are not guaranteed to compare equal (the compiler may duplicate or merge
/// functions), and Miri in fact fails such comparisons. A registry keyed by the private-data
/// address is exact: entries are removed in `release`, so an address cannot be occupied by a
/// foreign allocation while it is in the set.
fn exported_streams() -> std::sync::LockResult<std::sync::MutexGuard<'static, BTreeSet<usize>>> {
    static EXPORTED_STREAMS: Mutex<BTreeSet<usize>> = Mutex::new(BTreeSet::new());
    EXPORTED_STREAMS.lock()
}

struct StreamPrivateData {
    reader: Box<dyn RecordBatchReader + Send>,
    /// The last error's message, for `get_last_error`.
    last_message: Option<CString>,
    /// The last error's ADBC status, for [`error_from_array_stream`].
    last_status: AdbcStatusCode,
    /// The last error in full, for [`error_from_array_stream`]. An empty [`Default`] error
    /// until a callback fails.
    last_error: FFI_AdbcError,
}

/// The driver's ADBC 1.1.0 `ErrorFromArrayStream` implementation.
///
/// For a stream exported through [`export_reader`], returns the rich error stashed by the
/// last failed callback (an empty error with `ADBC_STATUS_OK` while no call has failed) and
/// writes its status code to `status` (if not NULL). Returns NULL for foreign or already
/// released streams.
///
/// Per the ADBC specification the returned error remains owned by the stream: the caller
/// must not release it, and it is invalidated when the stream itself is released.
pub(crate) unsafe extern "C" fn error_from_array_stream(
    stream: *mut FFI_ArrowArrayStream,
    status: *mut AdbcStatusCode,
) -> *const FFI_AdbcError {
    let Some(stream) = (unsafe { stream.as_mut() }) else {
        return std::ptr::null();
    };
    let is_exported_here = exported_streams()
        .expect("Poisoned exported-streams registry")
        .contains(&(stream.private_data as usize));
    if !is_exported_here {
        return std::ptr::null();
    }
    let data = stream.private_data as *mut StreamPrivateData;
    if !status.is_null() {
        unsafe { std::ptr::write_unaligned(status, (*data).last_status) };
    }
    // Hand out a raw-derived pointer, not `&(*data).last_error`: consumers (e.g. the Python
    // bindings' `convert_error`) call the error's own release callback through this pointer,
    // which mutates the error in place — undefined behavior through a shared reference.
    unsafe { &raw mut (*data).last_error as *const FFI_AdbcError }
}

/// The rich ADBC error behind `err`: the first [`adbc_core::error::Error`] in its
/// [source chain](std::error::Error::source), or an error synthesized from the [`ArrowError`]
/// variant when the chain carries none.
fn adbc_error(err: &ArrowError) -> Error {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(error) = source {
        if let Some(adbc) = error.downcast_ref::<Error>() {
            return adbc.clone();
        }
        source = error.source();
    }
    let status = match err {
        ArrowError::NotYetImplemented(_) => Status::NotImplemented,
        ArrowError::IoError(_, _) => Status::IO,
        _ => Status::Internal,
    };
    Error::with_message_and_status(err.to_string(), status)
}

/// A direct port of the C driver framework's `InternalAdbcStatusCodeToErrno`
/// (`c/driver/common/utils.c`) — the canonical AdbcStatusCode → errno table that the in-tree
/// C/C++ drivers return from their stream callbacks.
fn errno_for_status(status: Status) -> i32 {
    match status {
        Status::Ok => 0,
        Status::Unknown => EIO,
        Status::NotImplemented => ENOTSUP,
        Status::NotFound => ENOENT,
        Status::AlreadyExists => EEXIST,
        Status::InvalidArguments | Status::InvalidState => EINVAL,
        Status::InvalidData | Status::Integrity | Status::Internal | Status::IO => EIO,
        Status::Cancelled => ECANCELED,
        Status::Timeout => ETIMEDOUT,
        Status::Unauthenticated | Status::Unauthorized => EACCES,
    }
}

fn private_data(stream: *mut FFI_ArrowArrayStream) -> &'static mut StreamPrivateData {
    unsafe { &mut *((*stream).private_data as *mut StreamPrivateData) }
}

/// Stash the rich error behind `err` for [`error_from_array_stream`] and `get_last_error`,
/// and return the errno to report through the stream callback.
fn set_error(data: &mut StreamPrivateData, err: &ArrowError) -> c_int {
    let error = adbc_error(err);
    let status = error.status;
    // A NUL byte inside the message would truncate it, never fail the conversion.
    let message = err.to_string().replace('\0', " ");
    data.last_message = Some(CString::new(message).expect("NUL bytes were replaced"));
    data.last_status = status.into();
    // Dropping the previous value releases any error stashed by an earlier failure.
    data.last_error = FFI_AdbcError::try_from(error).unwrap_or_else(Into::into);
    errno_for_status(status)
}

unsafe extern "C" fn get_schema(
    stream: *mut FFI_ArrowArrayStream,
    out: *mut FFI_ArrowSchema,
) -> c_int {
    let data = private_data(stream);
    match FFI_ArrowSchema::try_from(data.reader.schema().as_ref()) {
        Ok(schema) => {
            unsafe { std::ptr::copy(addr_of!(schema), out, 1) };
            std::mem::forget(schema);
            0
        }
        Err(ref err) => set_error(data, err),
    }
}

unsafe extern "C" fn get_next(
    stream: *mut FFI_ArrowArrayStream,
    out: *mut FFI_ArrowArray,
) -> c_int {
    let data = private_data(stream);
    match data.reader.next() {
        // End of stream: a released ArrowArray marks completion.
        None => {
            unsafe { std::ptr::write(out, FFI_ArrowArray::empty()) };
            0
        }
        Some(Ok(batch)) => {
            let array = FFI_ArrowArray::new(&StructArray::from(batch).to_data());
            unsafe { std::ptr::write_unaligned(out, array) };
            0
        }
        Some(Err(ref err)) => set_error(data, err),
    }
}

unsafe extern "C" fn get_last_error(stream: *mut FFI_ArrowArrayStream) -> *const c_char {
    match &private_data(stream).last_message {
        Some(err) => err.as_ptr(),
        None => std::ptr::null(),
    }
}

unsafe extern "C" fn release_stream(stream: *mut FFI_ArrowArrayStream) {
    if stream.is_null() {
        return;
    }
    let stream = unsafe { &mut *stream };
    stream.get_schema = None;
    stream.get_next = None;
    stream.get_last_error = None;
    exported_streams()
        .expect("Poisoned exported-streams registry")
        .remove(&(stream.private_data as usize));
    // Also releases the stashed error, invalidating pointers previously handed out by
    // `error_from_array_stream`, as the ADBC specification prescribes.
    drop(unsafe { Box::from_raw(stream.private_data as *mut StreamPrivateData) });
    stream.private_data = std::ptr::null_mut();
    stream.release = None;
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::sync::Arc;

    use adbc_core::constants::{
        ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA, ADBC_STATUS_CANCELLED, ADBC_STATUS_IO,
    };
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};

    use crate::types::ErrorPrivateData;

    use super::*;

    /// A reader yielding the given batches and errors in order.
    struct FailingReader {
        schema: SchemaRef,
        items: std::vec::IntoIter<Result<RecordBatch, ArrowError>>,
    }

    impl Iterator for FailingReader {
        type Item = Result<RecordBatch, ArrowError>;
        fn next(&mut self) -> Option<Self::Item> {
            self.items.next()
        }
    }

    impl RecordBatchReader for FailingReader {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
    }

    fn reader_with(items: Vec<Result<RecordBatch, ArrowError>>) -> Box<FailingReader> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        Box::new(FailingReader {
            schema,
            items: items.into_iter(),
        })
    }

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap()
    }

    fn cancelled_error() -> ArrowError {
        ArrowError::ExternalError(Box::new(Error::with_message_and_status(
            "operation cancelled",
            Status::Cancelled,
        )))
    }

    /// Drive the exported stream exactly as a C consumer would, collecting each get_next errno.
    fn drive(stream: &mut FFI_ArrowArrayStream, calls: usize) -> Vec<i32> {
        let get_next = stream.get_next.unwrap();
        (0..calls)
            .map(|_| {
                let mut out = FFI_ArrowArray::empty();
                unsafe { get_next(stream as *mut _, &mut out as *mut _) }
            })
            .collect()
    }

    /// Call the driver's `ErrorFromArrayStream` exactly as the driver manager would.
    fn rich_error(
        stream: &mut FFI_ArrowArrayStream,
    ) -> (*const FFI_AdbcError, Option<AdbcStatusCode>) {
        // A sentinel that no ADBC status code uses, to detect whether `status` was written.
        let mut status: AdbcStatusCode = u8::MAX;
        let error = unsafe { error_from_array_stream(stream as *mut _, &mut status as *mut _) };
        (error, (status != u8::MAX).then_some(status))
    }

    #[test]
    fn stream_ends_cleanly() {
        let mut stream = export_reader(reader_with(vec![Ok(batch())]));
        assert_eq!(drive(&mut stream, 2), vec![0, 0]);
    }

    #[test]
    fn cancelled_adbc_error_maps_to_ecanceled() {
        let mut stream = export_reader(reader_with(vec![Ok(batch()), Err(cancelled_error())]));
        assert_eq!(drive(&mut stream, 2), vec![0, ECANCELED]);
        // The message is preserved for the consumer's get_last_error.
        let last_error = stream.get_last_error.unwrap();
        let message = unsafe { CStr::from_ptr(last_error(&mut stream as *mut _)) };
        assert!(message.to_string_lossy().contains("operation cancelled"));
    }

    #[test]
    fn rich_error_is_available_through_error_from_array_stream() {
        let failure = ArrowError::ExternalError(Box::new(Error {
            message: "operation cancelled".into(),
            status: Status::Cancelled,
            vendor_code: ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA,
            sqlstate: [b'5' as _, b'7' as _, b'0' as _, b'1' as _, b'4' as _],
            details: Some(vec![("reason".into(), b"user requested".to_vec())]),
        }));
        let mut stream = export_reader(reader_with(vec![Err(failure)]));
        assert_eq!(drive(&mut stream, 1), vec![ECANCELED]);

        let (error, status) = rich_error(&mut stream);
        assert_eq!(status, Some(ADBC_STATUS_CANCELLED));
        let error = unsafe { error.as_ref() }.unwrap();
        let message = unsafe { CStr::from_ptr(error.message) };
        assert!(message.to_string_lossy().contains("operation cancelled"));
        assert_eq!(error.vendor_code, ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA);
        assert_eq!(
            error.sqlstate,
            [b'5' as _, b'7' as _, b'0' as _, b'1' as _, b'4' as _]
        );
        // The 1.1.0 key/value details survive the conversion.
        let details = unsafe { &*(error.private_data as *const ErrorPrivateData) };
        assert_eq!(details.keys, vec![CString::new("reason").unwrap()]);
        assert_eq!(details.values, vec![b"user requested".to_vec()]);
    }

    #[test]
    fn consumer_may_release_the_returned_error_early() {
        // The ADBC spec says the stream owns the error returned by ErrorFromArrayStream, but
        // the Python bindings' `convert_error` releases it anyway after copying its contents.
        // That must not double-free when the stream is released afterwards.
        let mut stream = export_reader(reader_with(vec![Err(cancelled_error())]));
        assert_eq!(drive(&mut stream, 1), vec![ECANCELED]);

        let (error, _) = rich_error(&mut stream);
        let error = error as *mut FFI_AdbcError;
        unsafe { ((*error).release.unwrap())(error) };
        // The stream is dropped (released) here, freeing the stashed error a second time.
    }

    #[test]
    fn errors_without_an_adbc_status_are_synthesized() {
        let mut stream = export_reader(reader_with(vec![Err(ArrowError::IoError(
            "connection reset".into(),
            std::io::Error::other("connection reset"),
        ))]));
        assert_eq!(drive(&mut stream, 1), vec![EIO]);

        let (error, status) = rich_error(&mut stream);
        assert_eq!(status, Some(ADBC_STATUS_IO));
        let error = unsafe { error.as_ref() }.unwrap();
        let message = unsafe { CStr::from_ptr(error.message) };
        assert!(message.to_string_lossy().contains("connection reset"));
    }

    #[test]
    fn stream_without_failures_reports_ok() {
        let mut stream = export_reader(reader_with(vec![Ok(batch())]));
        let (error, status) = rich_error(&mut stream);
        assert_eq!(status, Some(ADBC_STATUS_OK));
        let error = unsafe { error.as_ref() }.unwrap();
        assert!(error.message.is_null());
    }

    #[test]
    fn foreign_and_released_streams_are_not_recognized() {
        // A stream exported by arrow-rs, not by this module.
        let mut foreign = FFI_ArrowArrayStream::new(reader_with(vec![]));
        let (error, status) = rich_error(&mut foreign);
        assert!(error.is_null());
        assert_eq!(status, None, "status must not be written");

        // A stream exported by this module, after release.
        let mut released = export_reader(reader_with(vec![]));
        unsafe { released.release.unwrap()(&mut released as *mut _) };
        let (error, status) = rich_error(&mut released);
        assert!(error.is_null());
        assert_eq!(status, None, "status must not be written");
    }

    #[test]
    fn wrapped_cancelled_error_is_found_through_the_source_chain() {
        // A Cancelled ADBC error nested one level deeper still maps to ECANCELED.
        #[derive(Debug)]
        struct Wrapper(Error);
        impl std::fmt::Display for Wrapper {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "wrapped: {}", self.0)
            }
        }
        impl std::error::Error for Wrapper {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }
        let err = ArrowError::ExternalError(Box::new(Wrapper(Error::with_message_and_status(
            "cancelled",
            Status::Cancelled,
        ))));
        assert_eq!(adbc_error(&err).status, Status::Cancelled);
    }

    #[test]
    fn arrow_errors_synthesize_a_status() {
        let synthesized = |err: &ArrowError| adbc_error(err).status;
        assert_eq!(
            synthesized(&ArrowError::NotYetImplemented("x".into())),
            Status::NotImplemented
        );
        assert_eq!(
            synthesized(&ArrowError::IoError("x".into(), std::io::Error::other("x"))),
            Status::IO
        );
        assert_eq!(
            synthesized(&ArrowError::ComputeError("x".into())),
            Status::Internal
        );
    }

    #[test]
    fn adbc_statuses_map_through_the_canonical_table() {
        // Spot-check the InternalAdbcStatusCodeToErrno port (c/driver/common/utils.c).
        assert_eq!(errno_for_status(Status::Cancelled), ECANCELED);
        assert_eq!(errno_for_status(Status::NotFound), ENOENT);
        assert_eq!(errno_for_status(Status::NotImplemented), ENOTSUP);
        assert_eq!(errno_for_status(Status::AlreadyExists), EEXIST);
        assert_eq!(errno_for_status(Status::Timeout), ETIMEDOUT);
        assert_eq!(errno_for_status(Status::Unauthorized), EACCES);
        assert_eq!(errno_for_status(Status::InvalidArguments), EINVAL);
        assert_eq!(errno_for_status(Status::Internal), EIO);
    }

    #[test]
    fn schema_round_trips() {
        let mut stream = export_reader(reader_with(vec![]));
        let get_schema = stream.get_schema.unwrap();
        let mut out = FFI_ArrowSchema::empty();
        assert_eq!(
            unsafe { get_schema(&mut stream as *mut _, &mut out as *mut _) },
            0
        );
    }
}
