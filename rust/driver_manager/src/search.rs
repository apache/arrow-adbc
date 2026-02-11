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

use std::borrow::Cow;
use std::ffi::{c_void, OsStr};
use std::fs;
use std::path::{Path, PathBuf};
use std::{env, ops};

use libloading::Symbol;
use toml::de::{DeTable, DeValue};

use adbc_core::{
    error::{Error, Result, Status},
    options::AdbcVersion,
    LoadFlags, LOAD_FLAG_ALLOW_RELATIVE_PATHS, LOAD_FLAG_SEARCH_ENV, LOAD_FLAG_SEARCH_SYSTEM,
    LOAD_FLAG_SEARCH_USER,
};
use adbc_ffi::{
    options::check_status,
    {FFI_AdbcDriver, FFI_AdbcDriverInitFunc, FFI_AdbcError},
};

use crate::error::libloading_error_to_adbc_error;

#[derive(Debug, Default)]
pub(crate) struct DriverInfo {
    pub(crate) lib_path: PathBuf,
    pub(crate) entrypoint: Option<Vec<u8>>,
    // TODO: until we add more logging these are unused so we'll leave
    // them out until such time that we do so.
    // driver_name: String,
    // version: String,
    // source: String,
}

impl DriverInfo {
    pub fn load_driver_manifest(manifest_file: &Path) -> Result<DriverInfo> {
        let contents = fs::read_to_string(manifest_file).map_err(|e| {
            Error::with_message_and_status(
                format!("Could not read manifest '{}': {e}", manifest_file.display()),
                Status::InvalidArguments,
            )
        })?;

        let manifest = DeTable::parse(&contents)
            .map_err(|e| Error::with_message_and_status(e.to_string(), Status::InvalidArguments))?;

        let manifest_version = manifest
            .get_ref()
            .get("manifest_version")
            .and_then(|v| v.get_ref().as_integer())
            .map(|v| v.as_str())
            .unwrap_or("1");

        if manifest_version != "1" {
            return Err(Error::with_message_and_status(
                format!("Unsupported manifest version: {manifest_version}"),
                Status::InvalidArguments,
            ));
        }

        // leave these out until we add logging that would actually utilize them
        // let driver_name = get_optional_key(&manifest, "name");
        // let version = get_optional_key(&manifest, "version");
        // let source = get_optional_key(&manifest, "source");
        let (os, arch, extra) = arch_triplet();

        // TODO(felipecrv): point to toml file line/column in error messages
        let driver_spanned = manifest.get_ref().get("Driver");
        let lib_path = match driver_spanned
            .and_then(|v| v.get_ref().get("shared"))
            .map(|v| v.get_ref())
        {
            // If the value is a string, we assume it is the path to the shared library.
            Some(DeValue::String(path)) => Ok(PathBuf::from(path.as_ref())),
            // If the value is a table, we look for the specific key for this OS and architecture.
            Some(DeValue::Table(driver)) => {
                let key = format!("{os}_{arch}{extra}");
                let path = driver
                    .get(key.as_str())
                    .and_then(|v| v.get_ref().as_str())
                    .unwrap_or_default();
                Ok(PathBuf::from(path))
            }
            Some(driver) => Err(Error::with_message_and_status(
                format!(
                    "Driver.shared must be a string or a table, found '{}'",
                    driver.type_str()
                ),
                Status::InvalidArguments,
            )),
            None => Err(Error::with_message_and_status(
                format!(
                    "Manifest '{}' missing Driver.shared key",
                    manifest_file.display()
                ),
                Status::InvalidArguments,
            )),
        }?;

        if lib_path.as_os_str().is_empty() {
            return Err(Error::with_message_and_status(
                format!(
                    "Manifest '{}' found empty string for library path of Driver.shared.{os}_{arch}{extra}",
                    lib_path.display()
                ),
                Status::InvalidArguments,
            ));
        }

        let entrypoint = driver_spanned
            .and_then(|driver| driver.get_ref().as_table())
            .and_then(|table| table.get("entrypoint"))
            .map(|spanned| {
                let val = spanned.get_ref();
                match val.as_str() {
                    Some(s) => Ok(s.as_bytes().to_vec()),
                    None => Err(Error::with_message_and_status(
                        "Driver entrypoint must be a string".to_string(),
                        Status::InvalidArguments,
                    )),
                }
            })
            .transpose()?;

        Ok(DriverInfo {
            lib_path,
            entrypoint,
        })
    }

    #[cfg(target_os = "windows")]
    pub(crate) fn load_from_registry(
        root: &windows_registry::Key,
        driver_name: &OsStr,
    ) -> Result<DriverInfo> {
        const ADBC_DRIVER_REGISTRY: &str = "SOFTWARE\\ADBC\\Drivers";
        let drivers_key = root
            .open(ADBC_DRIVER_REGISTRY)
            .and_then(|k| k.open(driver_name.to_str().unwrap_or_default()))
            .map_err(|e| {
                Error::with_message_and_status(
                    format!("Failed to open registry key: {e}"),
                    Status::NotFound,
                )
            })?;

        let manifest_version = drivers_key.get_u32("manifest_version").unwrap_or(1);

        if manifest_version != 1 {
            return Err(Error::with_message_and_status(
                format!("Unsupported manifest version: {manifest_version}"),
                Status::InvalidArguments,
            ));
        }

        let entrypoint = drivers_key
            .get_string("entrypoint")
            .ok()
            .map(|s| s.into_bytes());

        Ok(DriverInfo {
            lib_path: PathBuf::from(drivers_key.get_string("driver").unwrap_or_default()),
            entrypoint,
        })
    }
}

pub(crate) struct SearchHit {
    /// The path where `library` was loaded from.
    pub lib_path: PathBuf,
    /// The loaded library.
    pub library: libloading::Library,
    /// The entrypoint from the manifest file or registry if specified.
    ///
    /// Must have priority over a specified entrypoint or the one derived
    /// from the library name.
    pub entrypoint: Option<Vec<u8>>,
}

impl SearchHit {
    pub fn new(
        lib_path: PathBuf,
        library: libloading::Library,
        entrypoint: Option<Vec<u8>>,
    ) -> Self {
        Self {
            lib_path,
            library,
            entrypoint,
        }
    }

    pub fn resolve_entrypoint<'a>(&'a self, entrypoint: Option<&'a [u8]>) -> Cow<'a, [u8]> {
        if let Some(entrypoint) = self.entrypoint.as_deref() {
            // prioritize manifest entrypoint..
            Cow::Borrowed(entrypoint)
        } else {
            // ...over the provided one
            DriverLibrary::derive_entrypoint(entrypoint, &self.lib_path)
        }
    }
}

enum DriverInitFunc<'a> {
    /// The driver initialization function as a static function pointer.
    Static(&'a FFI_AdbcDriverInitFunc),
    /// The driver initialization function as a [Symbol] from a dynamically loaded library.
    Dynamic(Symbol<'a, FFI_AdbcDriverInitFunc>),
}

/// Allow using [DriverInitFunc] as a function pointer.
impl<'a> ops::Deref for DriverInitFunc<'a> {
    type Target = FFI_AdbcDriverInitFunc;

    fn deref(&self) -> &Self::Target {
        match self {
            DriverInitFunc::Static(init) => init,
            DriverInitFunc::Dynamic(init) => init,
        }
    }
}

pub(crate) struct DriverLibrary<'a> {
    init: DriverInitFunc<'a>,
}

impl<'a> DriverLibrary<'a> {
    pub(crate) fn from_static_init(init: &'a FFI_AdbcDriverInitFunc) -> Self {
        Self {
            init: DriverInitFunc::Static(init),
        }
    }

    pub(crate) fn try_from_dynamic_library(
        library: &'a libloading::Library,
        entrypoint: &[u8],
    ) -> Result<Self> {
        let init: libloading::Symbol<adbc_ffi::FFI_AdbcDriverInitFunc> = unsafe {
            library
                .get(entrypoint)
                .or_else(|_| library.get(b"AdbcDriverInit"))
                .map_err(libloading_error_to_adbc_error)?
        };
        let init = DriverInitFunc::Dynamic(init);
        Ok(Self { init })
    }

    /// Initialize the driver via the library's entrypoint.
    pub(crate) fn init_driver(&self, version: AdbcVersion) -> Result<FFI_AdbcDriver> {
        let mut error = FFI_AdbcError::default();
        let mut driver = FFI_AdbcDriver::default();
        let status = unsafe {
            (self.init)(
                version.into(),
                &mut driver as *mut FFI_AdbcDriver as *mut c_void,
                &mut error,
            )
        };
        check_status(status, error)?;
        Ok(driver)
    }

    pub(crate) fn load_library(filename: impl AsRef<OsStr>) -> Result<libloading::Library> {
        eprintln!(
            "Trying to load library from path: {}",
            filename.as_ref().to_string_lossy()
        );
        // By default, go builds the libraries with '-Wl -z nodelete' which does not
        // unload the go runtime. This isn't respected on mac ( https://github.com/golang/go/issues/11100#issuecomment-932638093 )
        // so we need to explicitly load the library with RTLD_NODELETE( which prevents unloading )
        #[cfg(unix)]
        let library: libloading::Library = unsafe {
            use std::os::raw::c_int;

            const RTLD_NODELETE: c_int = if cfg!(any(
                target_os = "macos",
                target_os = "ios",
                target_os = "tvos",
                target_os = "visionos",
                target_os = "watchos",
            )) {
                0x80
            } else if cfg!(any(
                target_os = "linux",
                target_os = "android",
                target_os = "emscripten",
                target_os = "freebsd",
                target_os = "dragonfly",
                target_os = "openbsd",
                target_os = "haiku",
                target_os = "solaris",
                target_os = "illumos",
                target_env = "uclibc",
                target_env = "newlib",
                target_os = "fuchsia",
                target_os = "redox",
                target_os = "hurd",
                target_os = "cygwin",
            )) {
                0x1000
            } else {
                0x0
            };

            libloading::os::unix::Library::open(
                Some(filename.as_ref()),
                libloading::os::unix::RTLD_LAZY | libloading::os::unix::RTLD_LOCAL | RTLD_NODELETE,
            )
            .map(Into::into)
            .map_err(libloading_error_to_adbc_error)?
        };
        // on windows, we emulate the same behaviour by using the GET_MODULE_HANDLE_EX_FLAG_PIN. The `.pin()`
        // function implements this.
        #[cfg(windows)]
        let library: libloading::Library = unsafe {
            let library: libloading::os::windows::Library =
                libloading::os::windows::Library::new(filename.as_ref())
                    .map_err(libloading_error_to_adbc_error)?;
            library.pin().map_err(libloading_error_to_adbc_error)?;
            library.into()
        };
        eprintln!("Successfully loaded library: {library:?}");
        Ok(library)
    }

    pub(crate) fn load_library_from_name(name: impl AsRef<str>) -> Result<libloading::Library> {
        let filename = libloading::library_filename(name.as_ref());
        Self::load_library(&filename)
    }

    pub(crate) fn load_library_from_manifest(manifest_file: &Path) -> Result<SearchHit> {
        let info = DriverInfo::load_driver_manifest(manifest_file)?;
        let library = Self::load_library(&info.lib_path)?;
        Ok(SearchHit::new(info.lib_path, library, info.entrypoint))
    }

    pub(crate) fn derive_entrypoint<'b>(
        entrypoint: Option<&'b [u8]>,
        driver_path: impl AsRef<OsStr>,
    ) -> Cow<'b, [u8]> {
        if let Some(entrypoint) = entrypoint {
            Cow::Borrowed(entrypoint)
        } else {
            Cow::Owned(Self::get_default_entrypoint(driver_path).into_bytes())
        }
    }

    pub(crate) fn derive_entrypoint_from_name<'b>(
        entrypoint: Option<&'b [u8]>,
        name: &str,
    ) -> Cow<'b, [u8]> {
        if let Some(entrypoint) = entrypoint {
            Cow::Borrowed(entrypoint)
        } else {
            Cow::Owned(Self::get_default_entrypoint_from_name(name).into_bytes())
        }
    }

    pub(crate) fn search(
        name: impl AsRef<OsStr>,
        load_flags: LoadFlags,
        additional_search_paths: Option<Vec<PathBuf>>,
        trace: &mut Vec<Error>,
    ) -> Result<SearchHit> {
        let driver_path = Path::new(name.as_ref());
        let allow_relative = load_flags & LOAD_FLAG_ALLOW_RELATIVE_PATHS != 0;

        if let Some(ext) = driver_path.extension() {
            if !allow_relative && driver_path.is_relative() {
                Err(Error::with_message_and_status(
                    "Relative paths are not allowed",
                    Status::InvalidArguments,
                ))
            } else if ext == "toml" {
                DriverLibrary::load_library_from_manifest(driver_path)
            } else {
                let library = DriverLibrary::load_library(driver_path)?;
                Ok(SearchHit::new(driver_path.to_path_buf(), library, None))
            }
        } else if driver_path.is_absolute() {
            let toml_path = driver_path.with_extension("toml");
            if toml_path.is_file() {
                DriverLibrary::load_library_from_manifest(&toml_path)
            } else {
                let library = DriverLibrary::load_library(driver_path)?;
                Ok(SearchHit::new(driver_path.to_path_buf(), library, None))
            }
        } else {
            DriverLibrary::find_driver(driver_path, load_flags, additional_search_paths, trace)
        }
    }

    #[cfg(target_os = "windows")]
    pub(crate) fn load_library_from_registry(
        root: &windows_registry::Key,
        driver_name: &OsStr,
    ) -> Result<SearchHit> {
        let info = DriverInfo::load_from_registry(root, driver_name)?;
        let library = Self::load_library(&info.lib_path)?;
        Ok(SearchHit::new(info.lib_path, library, info.entrypoint))
    }

    #[cfg(target_os = "windows")]
    pub(crate) fn find_driver(
        driver_path: &Path,
        load_flags: LoadFlags,
        additional_search_paths: Option<Vec<PathBuf>>,
        trace: &mut Vec<Error>,
    ) -> Result<SearchHit> {
        if load_flags & LOAD_FLAG_SEARCH_ENV != 0 {
            if let Ok(result) = DriverLibrary::search_path_list(
                driver_path,
                &get_search_paths(LOAD_FLAG_SEARCH_ENV),
                trace,
            ) {
                return Ok(result);
            }
        }

        // the logic we want is that we first search ADBC_DRIVER_PATH if set,
        // then we search the additional search paths if they exist. Finally,
        // we will search CONDA_PREFIX if built with conda_build before moving on.
        if let Some(additional_search_paths) = additional_search_paths {
            if let Ok(result) =
                DriverLibrary::search_path_list(driver_path, &additional_search_paths, trace)
            {
                return Ok(result);
            }
        }

        #[cfg(conda_build)]
        if load_flags & LOAD_FLAG_SEARCH_ENV != 0 {
            if let Some(conda_prefix) = env::var_os("CONDA_PREFIX") {
                let conda_path = PathBuf::from(conda_prefix)
                    .join("etc")
                    .join("adbc")
                    .join("drivers");
                if let Ok(result) =
                    DriverLibrary::search_path_list(driver_path, &[conda_path], trace)
                {
                    return Ok(result);
                }
            }
        }

        if load_flags & LOAD_FLAG_SEARCH_USER != 0 {
            // first check registry for the driver, then check the user config path
            let result = DriverLibrary::load_library_from_registry(
                windows_registry::CURRENT_USER,
                driver_path.as_os_str(),
            );
            if result.is_ok() {
                return result;
            }

            if let Ok(result) = DriverLibrary::search_path_list(
                driver_path,
                &get_search_paths(LOAD_FLAG_SEARCH_USER),
                trace,
            ) {
                return Ok(result);
            }
        }

        if load_flags & LOAD_FLAG_SEARCH_SYSTEM != 0 {
            let result = DriverLibrary::load_library_from_registry(
                windows_registry::LOCAL_MACHINE,
                driver_path.as_os_str(),
            );
            if result.is_ok() {
                return result;
            }

            if let Ok(result) = DriverLibrary::search_path_list(
                driver_path,
                &get_search_paths(LOAD_FLAG_SEARCH_SYSTEM),
                trace,
            ) {
                return Ok(result);
            }
        }

        let driver_name = driver_path.as_os_str().to_string_lossy().into_owned();
        let library = DriverLibrary::load_library_from_name(driver_name)?;
        Ok(SearchHit::new(driver_path.to_path_buf(), library, None))
        // XXX: should we return NotFound like the non-Windows version?
    }

    #[cfg(not(windows))]
    pub(crate) fn find_driver(
        driver_path: &Path,
        load_flags: LoadFlags,
        additional_search_paths: Option<Vec<PathBuf>>,
        trace: &mut Vec<Error>,
    ) -> Result<SearchHit> {
        let mut path_list = get_search_paths(load_flags & LOAD_FLAG_SEARCH_ENV);

        if let Some(additional_search_paths) = additional_search_paths {
            path_list.extend(additional_search_paths);
        }

        #[cfg(conda_build)]
        if load_flags & LOAD_FLAG_SEARCH_ENV != 0 {
            if let Some(conda_prefix) = env::var_os("CONDA_PREFIX") {
                let conda_path = PathBuf::from(conda_prefix)
                    .join("etc")
                    .join("adbc")
                    .join("drivers");
                path_list.push(conda_path);
            }
        }

        path_list.extend(get_search_paths(load_flags & !LOAD_FLAG_SEARCH_ENV));
        if let Ok(result) = DriverLibrary::search_path_list(driver_path, &path_list, trace) {
            return Ok(result);
        }

        // Convert OsStr to String before passing to load_dynamic_from_name
        let driver_name = driver_path.as_os_str().to_string_lossy().into_owned();
        match DriverLibrary::load_library_from_name(driver_name) {
            Ok(library) => return Ok(SearchHit::new(driver_path.to_path_buf(), library, None)),
            Err(err) => trace.push(err),
        }

        Err(Error::with_message_and_status(
            format!("Driver not found: {}", driver_path.display()),
            Status::NotFound,
        ))
    }

    /// Search for the driver library in the given list of paths.
    ///
    /// `driver_path` can be a library name or a manifest file name. The search loop will also try
    /// changing the extension to `.toml` and try to load it as a manifest before trying to load it
    /// directly as a dynamic library.
    ///
    /// The first successful load will return the library path, the loaded library, and an optional
    /// entrypoint defined in the manifest in case it was loaded from one that specified it.
    pub(crate) fn search_path_list(
        driver_path: &Path,
        path_list: &[PathBuf],
        trace: &mut Vec<Error>,
    ) -> Result<SearchHit> {
        path_list
            .iter()
            .find_map(|path| {
                let mut full_path = path.join(driver_path);
                full_path.set_extension("toml");
                if full_path.is_file() {
                    match DriverLibrary::load_library_from_manifest(&full_path) {
                        Ok(hit) => return Some(Ok(hit)),
                        Err(err) => trace.push(err),
                    }
                }

                full_path.set_extension(""); // Remove the extension to try loading as a dynamic library.
                let result = DriverLibrary::load_library(&full_path)
                    .map(|library| SearchHit::new(full_path, library, None));
                match result {
                    Ok(res) => return Some(Ok(res)),
                    Err(err) => trace.push(err),
                }
                None
            })
            .unwrap_or_else(|| {
                Err(Error::with_message_and_status(
                    format!("Driver not found: {}", driver_path.display()),
                    Status::NotFound,
                ))
            })
    }

    /// Construct default entrypoint from the library path.
    pub(crate) fn get_default_entrypoint(driver_path: impl AsRef<OsStr>) -> String {
        // - libadbc_driver_sqlite.so.2.0.0 -> AdbcDriverSqliteInit
        // - adbc_driver_sqlite.dll -> AdbcDriverSqliteInit
        // - proprietary_driver.dll -> AdbcProprietaryDriverInit

        // potential path -> filename
        let filename = driver_path.as_ref().to_str().unwrap_or_default();
        let filename = filename
            .rfind(['/', '\\'])
            .map_or(filename, |pos| &filename[pos + 1..]);
        // remove all extensions
        let basename = filename
            .find('.')
            .map_or_else(|| filename, |pos| &filename[..pos]);
        // strip the lib prefix on unix-like systems
        let name = basename
            .strip_prefix(env::consts::DLL_PREFIX)
            .unwrap_or(basename);
        Self::get_default_entrypoint_from_name(name)
    }

    /// Construct default entrypoint from the library name.
    pub(crate) fn get_default_entrypoint_from_name(name: &str) -> String {
        let entrypoint = name
            .split(&['-', '_'][..])
            .map(|s| {
                // uppercase first character of a string
                // https://stackoverflow.com/questions/38406793/why-is-capitalizing-the-first-letter-of-a-string-so-convoluted-in-rust
                let mut c = s.chars();
                match c.next() {
                    None => String::new(),
                    Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
                }
            })
            .collect::<Vec<_>>()
            .join("");

        if entrypoint.starts_with("Adbc") {
            format!("{entrypoint}Init")
        } else {
            format!("Adbc{entrypoint}Init")
        }
    }
}

pub(crate) const fn arch_triplet() -> (&'static str, &'static str, &'static str) {
    #[cfg(target_arch = "x86_64")]
    const ARCH: &str = "amd64";
    #[cfg(all(target_arch = "aarch64", target_endian = "big"))]
    const ARCH: &str = "arm64be";
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    const ARCH: &str = "arm64";
    #[cfg(all(target_arch = "powerpc64", target_endian = "little"))]
    const ARCH: &str = "powerpc64le";
    #[cfg(all(target_arch = "powerpc64", target_endian = "big"))]
    const ARCH: &str = "powerpc64";
    #[cfg(target_arch = "riscv32")]
    const ARCH: &str = "riscv";
    #[cfg(not(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "powerpc64",
        target_arch = "riscv32",
    )))]
    const ARCH: &str = std::env::consts::ARCH;

    const OS: &str = std::env::consts::OS;

    #[cfg(target_env = "musl")]
    const EXTRA: &str = "_musl";
    #[cfg(all(target_os = "windows", target_env = "gnu"))]
    const EXTRA: &str = "_mingw";
    #[cfg(not(any(target_env = "musl", all(target_os = "windows", target_env = "gnu"))))]
    const EXTRA: &str = "";

    (OS, ARCH, EXTRA)
}

#[cfg(target_os = "windows")]
pub(crate) mod target_windows {
    use windows_sys as windows;

    use std::ffi::c_void;
    use std::ffi::OsString;
    use std::os::windows::ffi::OsStringExt;
    use std::path::PathBuf;
    use std::slice;

    use windows::Win32::UI::Shell;

    // adapted from https://github.com/dirs-dev/dirs-sys-rs/blob/main/src/lib.rs#L150
    pub fn user_config_dir() -> Option<PathBuf> {
        unsafe {
            let mut path_ptr: windows::core::PWSTR = std::ptr::null_mut();
            let result = Shell::SHGetKnownFolderPath(
                &Shell::FOLDERID_LocalAppData,
                0,
                std::ptr::null_mut(),
                &mut path_ptr,
            );

            if result == 0 {
                let len = windows::Win32::Globalization::lstrlenW(path_ptr) as usize;
                let path = slice::from_raw_parts(path_ptr, len);
                let ostr: OsString = OsStringExt::from_wide(path);
                windows::Win32::System::Com::CoTaskMemFree(path_ptr as *const c_void);
                Some(PathBuf::from(ostr))
            } else {
                windows::Win32::System::Com::CoTaskMemFree(path_ptr as *const c_void);
                None
            }
        }
    }
}

pub(crate) fn user_config_dir() -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        use target_windows::user_config_dir;
        user_config_dir().map(|mut path| {
            path.push("ADBC");
            path.push("Drivers");
            path
        })
    }

    #[cfg(target_os = "macos")]
    {
        env::var_os("HOME").map(PathBuf::from).map(|mut path| {
            path.push("Library");
            path.push("Application Support");
            path.push("ADBC");
            path.push("Drivers");
            path
        })
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    {
        env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or_else(|| {
                env::var_os("HOME").map(|home| {
                    let mut path = PathBuf::from(home);
                    path.push(".config");
                    path
                })
            })
            .map(|mut path| {
                path.push("adbc");
                path.push("drivers");
                path
            })
    }
}

pub(crate) fn system_config_dir() -> Option<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        Some(PathBuf::from("/Library/Application Support/ADBC/Drivers"))
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    {
        Some(PathBuf::from("/etc/adbc/drivers"))
    }

    #[cfg(not(unix))]
    {
        None
    }
}

fn get_search_paths(lvls: LoadFlags) -> Vec<PathBuf> {
    let mut result = Vec::new();
    if lvls & LOAD_FLAG_SEARCH_ENV != 0 {
        if let Some(paths) = env::var_os("ADBC_DRIVER_PATH") {
            for p in env::split_paths(&paths) {
                result.push(p);
            }
        }
    }

    if lvls & LOAD_FLAG_SEARCH_USER != 0 {
        if let Some(path) = user_config_dir() {
            if path.exists() {
                result.push(path);
            }
        }
    }

    // system level for windows is to search the registry keys
    #[cfg(not(windows))]
    if lvls & LOAD_FLAG_SEARCH_SYSTEM != 0 {
        if let Some(path) = system_config_dir() {
            if path.exists() {
                result.push(path);
            }
        }
    }

    result
}

pub(crate) fn parse_driver_uri(uri: &str) -> Result<(&str, &str)> {
    let idx = uri.find(":").ok_or(Error::with_message_and_status(
        format!("Invalid URI: {uri}"),
        Status::InvalidArguments,
    ))?;

    let driver = &uri[..idx];
    if uri.len() <= idx + 2 {
        return Ok((driver, uri));
    }

    #[cfg(target_os = "windows")]
    if let Ok(true) = std::fs::exists(uri) {
        return Ok((uri, ""));
    }

    if &uri[idx..idx + 2] == ":/" {
        // scheme is also driver
        return Ok((driver, uri));
    }

    Ok((driver, &uri[idx + 1..]))
}

#[cfg(test)]
mod tests {
    use std::env;

    use adbc_core::{options::AdbcVersion, LOAD_FLAG_ALLOW_RELATIVE_PATHS};
    use temp_env::{with_var, with_var_unset};

    use crate::ManagedDriver;

    use super::*;

    fn manifest_without_driver() -> &'static str {
        r#"
        manifest_version = 1

        name = 'SQLite3'
        publisher = 'arrow-adbc'
        version = '1.0.0'

        [ADBC]
        version = '1.1.0'
        "#
    }

    fn simple_manifest() -> String {
        // if this test is enabled, we expect the env var ADBC_DRIVER_MANAGER_TEST_LIB
        // to be defined.
        let driver_path =
            PathBuf::from(env::var_os("ADBC_DRIVER_MANAGER_TEST_LIB").expect(
                "ADBC_DRIVER_MANAGER_TEST_LIB must be set for driver manager manifest tests",
            ));

        assert!(
            driver_path.exists(),
            "ADBC_DRIVER_MANAGER_TEST_LIB path does not exist: {}",
            driver_path.display()
        );

        let (os, arch, extra) = arch_triplet();
        format!(
            r#"
    {}

    [Driver]
    [Driver.shared]
    {os}_{arch}{extra} = {driver_path:?}
    "#,
            manifest_without_driver()
        )
    }

    fn write_manifest_to_tempfile(p: PathBuf, tbl: String) -> (tempfile::TempDir, PathBuf) {
        let tmp_dir = tempfile::Builder::new()
            .prefix("adbc_tests")
            .tempdir()
            .expect("Failed to create temporary directory for driver manager manifest test");

        let manifest_path = tmp_dir.path().join(p);
        if let Some(parent) = manifest_path.parent() {
            std::fs::create_dir_all(parent)
                .expect("Failed to create parent directory for manifest");
        }

        std::fs::write(&manifest_path, tbl.as_str())
            .expect("Failed to write driver manager manifest to temporary file");

        (tmp_dir, manifest_path)
    }

    #[cfg_attr(target_os = "windows", ignore)] // TODO: remove this line after fixing
    #[test]
    fn test_default_entrypoint() {
        for driver in [
            "adbc_driver_sqlite",
            "adbc_driver_sqlite.dll",
            "driver_sqlite",
            "libadbc_driver_sqlite",
            "libadbc_driver_sqlite.so",
            "libadbc_driver_sqlite.so.6.0.0",
            "/usr/lib/libadbc_driver_sqlite.so",
            "/usr/lib/libadbc_driver_sqlite.so.6.0.0",
            "C:\\System32\\adbc_driver_sqlite.dll",
        ] {
            assert_eq!(
                DriverLibrary::get_default_entrypoint(driver),
                "AdbcDriverSqliteInit"
            );
        }

        for driver in [
            "adbc_sqlite",
            "sqlite",
            "/usr/lib/sqlite.so",
            "C:\\System32\\sqlite.dll",
        ] {
            assert_eq!(
                DriverLibrary::get_default_entrypoint(driver),
                "AdbcSqliteInit"
            );
        }

        for driver in [
            "proprietary_engine",
            "libproprietary_engine.so.6.0.0",
            "/usr/lib/proprietary_engine.so",
            "C:\\System32\\proprietary_engine.dll",
        ] {
            assert_eq!(
                DriverLibrary::get_default_entrypoint(driver),
                "AdbcProprietaryEngineInit"
            );
        }

        for driver in ["driver_example", "libdriver_example.so"] {
            assert_eq!(
                DriverLibrary::get_default_entrypoint(driver),
                "AdbcDriverExampleInit"
            );
        }
    }

    #[cfg_attr(target_os = "windows", ignore)] // TODO: remove this line after fixing
    #[test]
    fn test_default_entrypoint_from_name() {
        for name in ["adbc_driver_sqlite", "driver_sqlite"] {
            assert_eq!(
                DriverLibrary::get_default_entrypoint_from_name(name),
                "AdbcDriverSqliteInit"
            );

            let filename = libloading::library_filename(name);
            assert_eq!(
                DriverLibrary::get_default_entrypoint(filename),
                "AdbcDriverSqliteInit"
            );
        }

        for name in ["adbc_sqlite", "sqlite"] {
            assert_eq!(
                DriverLibrary::get_default_entrypoint_from_name(name),
                "AdbcSqliteInit"
            );

            let filename = libloading::library_filename(name);
            assert_eq!(
                DriverLibrary::get_default_entrypoint(filename),
                "AdbcSqliteInit"
            );
        }
    }

    /// Regression test for https://github.com/apache/arrow-adbc/pull/3693
    /// Ensures driver manager tests for Windows pull in Windows crates. This
    /// can be removed/replace when more complete tests are added.
    #[test]
    fn test_user_config_dir() {
        let _ = user_config_dir().unwrap();
    }

    #[test]
    #[cfg_attr(not(windows), ignore)]
    fn test_get_search_paths() {
        #[cfg(target_os = "macos")]
        let system_path = PathBuf::from("/Library/Application Support/ADBC/Drivers");
        #[cfg(not(target_os = "macos"))]
        let system_path = PathBuf::from("/etc/adbc/drivers");

        let search_paths = get_search_paths(LOAD_FLAG_SEARCH_SYSTEM);
        if system_path.exists() {
            assert_eq!(search_paths, vec![system_path]);
        } else {
            assert_eq!(search_paths, Vec::<PathBuf>::new());
        }
    }

    #[test]
    #[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
    fn test_load_relative_path() {
        std::fs::write(PathBuf::from("sqlite.toml"), simple_manifest())
            .expect("Failed to write driver manager manifest to file");

        let err = ManagedDriver::load_from_name("sqlite.toml", None, AdbcVersion::V100, 0, None)
            .unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);

        ManagedDriver::load_from_name(
            "sqlite.toml",
            None,
            AdbcVersion::V100,
            LOAD_FLAG_ALLOW_RELATIVE_PATHS,
            None,
        )
        .unwrap();

        std::fs::remove_file("sqlite.toml")
            .expect("Failed to remove temporary driver manifest file");
    }

    #[test]
    #[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
    fn test_manifest_missing_driver() {
        let (tmp_dir, manifest_path) = write_manifest_to_tempfile(
            PathBuf::from("sqlite.toml"),
            manifest_without_driver().to_string(),
        );

        let err = ManagedDriver::load_from_name(
            manifest_path,
            None,
            AdbcVersion::V100,
            adbc_core::LOAD_FLAG_DEFAULT,
            None,
        )
        .unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);

        tmp_dir
            .close()
            .expect("Failed to close/remove temporary directory");
    }

    #[test]
    #[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
    fn test_manifest_bad_version() {
        let manifest = "manifest_version = 2\n".to_owned() + &simple_manifest().to_owned();
        let (tmp_dir, manifest_path) =
            write_manifest_to_tempfile(PathBuf::from("sqlite.toml"), manifest);

        let err = ManagedDriver::load_from_name(
            manifest_path,
            None,
            AdbcVersion::V100,
            adbc_core::LOAD_FLAG_DEFAULT,
            None,
        )
        .unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);

        tmp_dir
            .close()
            .expect("Failed to close/remove temporary directory");
    }

    #[test]
    #[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
    fn test_manifest_wrong_arch() {
        let manifest_wrong_arch = format!(
            r#"
    {}

    [Driver]
    [Driver.shared]
    non-existing = 'path/to/bad/driver.so'
    "#,
            manifest_without_driver()
        );

        let (tmp_dir, manifest_path) =
            write_manifest_to_tempfile(PathBuf::from("sqlite.toml"), manifest_wrong_arch);

        let err = ManagedDriver::load_from_name(
            manifest_path,
            None,
            AdbcVersion::V100,
            adbc_core::LOAD_FLAG_DEFAULT,
            None,
        )
        .unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);

        tmp_dir
            .close()
            .expect("Failed to close/remove temporary directory");
    }

    #[test]
    #[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
    fn test_load_absolute_path() {
        let (tmp_dir, manifest_path) =
            write_manifest_to_tempfile(PathBuf::from("sqlite.toml"), simple_manifest());

        ManagedDriver::load_from_name(
            manifest_path,
            None,
            AdbcVersion::V100,
            adbc_core::LOAD_FLAG_DEFAULT,
            None,
        )
        .unwrap();

        tmp_dir
            .close()
            .expect("Failed to close/remove temporary directory");
    }

    #[test]
    #[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
    fn test_load_absolute_path_no_ext() {
        let (tmp_dir, mut manifest_path) =
            write_manifest_to_tempfile(PathBuf::from("sqlite.toml"), simple_manifest());

        manifest_path.set_extension("");
        ManagedDriver::load_from_name(
            manifest_path,
            None,
            AdbcVersion::V100,
            adbc_core::LOAD_FLAG_DEFAULT,
            None,
        )
        .unwrap();

        tmp_dir
            .close()
            .expect("Failed to close/remove temporary directory");
    }

    #[test]
    #[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
    #[cfg_attr(target_os = "windows", ignore)] // TODO: remove this line after fixing
    fn test_load_driver_env() {
        // ensure that we fail without the env var set
        with_var_unset("ADBC_DRIVER_PATH", || {
            let err = ManagedDriver::load_from_name(
                "sqlite",
                None,
                AdbcVersion::V100,
                adbc_core::LOAD_FLAG_SEARCH_ENV,
                None,
            )
            .unwrap_err();
            assert_eq!(err.status, Status::NotFound);
        });

        let (tmp_dir, manifest_path) =
            write_manifest_to_tempfile(PathBuf::from("sqlite.toml"), simple_manifest());

        with_var(
            "ADBC_DRIVER_PATH",
            Some(manifest_path.parent().unwrap().as_os_str()),
            || {
                ManagedDriver::load_from_name(
                    "sqlite",
                    None,
                    AdbcVersion::V100,
                    adbc_core::LOAD_FLAG_SEARCH_ENV,
                    None,
                )
                .unwrap();
            },
        );

        tmp_dir
            .close()
            .expect("Failed to close/remove temporary directory");
    }

    #[test]
    #[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
    fn test_load_driver_env_multiple_paths() {
        let (tmp_dir, manifest_path) =
            write_manifest_to_tempfile(PathBuf::from("sqlite.toml"), simple_manifest());

        let path_os_string = env::join_paths([
            Path::new("/home"),
            Path::new(""),
            manifest_path.parent().unwrap(),
        ])
        .unwrap();

        with_var("ADBC_DRIVER_PATH", Some(&path_os_string), || {
            ManagedDriver::load_from_name(
                "sqlite",
                None,
                AdbcVersion::V100,
                adbc_core::LOAD_FLAG_SEARCH_ENV,
                None,
            )
            .unwrap();
        });

        tmp_dir
            .close()
            .expect("Failed to close/remove temporary directory");
    }

    #[test]
    #[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
    fn test_load_additional_path() {
        let p = PathBuf::from("majestik møøse/sqlite.toml");
        let (tmp_dir, manifest_path) = write_manifest_to_tempfile(p, simple_manifest());

        ManagedDriver::load_from_name(
            "sqlite",
            None,
            AdbcVersion::V100,
            adbc_core::LOAD_FLAG_SEARCH_ENV,
            Some(vec![manifest_path.parent().unwrap().to_path_buf()]),
        )
        .unwrap();

        tmp_dir
            .close()
            .expect("Failed to close/remove temporary directory");
    }

    #[test]
    #[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
    fn test_load_non_ascii_path() {
        let p = PathBuf::from("majestik møøse/sqlite.toml");
        let (tmp_dir, manifest_path) = write_manifest_to_tempfile(p, simple_manifest());

        with_var(
            "ADBC_DRIVER_PATH",
            Some(manifest_path.parent().unwrap().as_os_str()),
            || {
                ManagedDriver::load_from_name(
                    "sqlite",
                    None,
                    AdbcVersion::V100,
                    adbc_core::LOAD_FLAG_SEARCH_ENV,
                    None,
                )
                .unwrap();
            },
        );

        tmp_dir
            .close()
            .expect("Failed to close/remove temporary directory");
    }

    #[test]
    #[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
    #[cfg_attr(target_os = "windows", ignore)] // TODO: remove this line after fixing
    fn test_disallow_env_config() {
        let (tmp_dir, manifest_path) =
            write_manifest_to_tempfile(PathBuf::from("sqlite.toml"), simple_manifest());

        with_var(
            "ADBC_DRIVER_PATH",
            Some(manifest_path.parent().unwrap().as_os_str()),
            || {
                let load_flags = adbc_core::LOAD_FLAG_DEFAULT & !adbc_core::LOAD_FLAG_SEARCH_ENV;
                let err = ManagedDriver::load_from_name(
                    "sqlite",
                    None,
                    AdbcVersion::V100,
                    load_flags,
                    None,
                )
                .unwrap_err();
                assert_eq!(err.status, Status::NotFound);
            },
        );

        tmp_dir
            .close()
            .expect("Failed to close/remove temporary directory");
    }

    #[test]
    #[cfg_attr(
        not(all(
            feature = "driver_manager_test_lib",
            feature = "driver_manager_test_manifest_user"
        )),
        ignore
    )]
    #[cfg_attr(target_os = "windows", ignore)] // TODO: remove this line after fixing
    fn test_manifest_user_config() {
        let err = ManagedDriver::load_from_name(
            "adbc-test-sqlite",
            None,
            AdbcVersion::V110,
            adbc_core::LOAD_FLAG_DEFAULT,
            None,
        )
        .unwrap_err();
        assert_eq!(err.status, Status::NotFound);

        let usercfg_dir = user_config_dir().unwrap();
        let mut created = false;
        if !usercfg_dir.exists() {
            std::fs::create_dir_all(&usercfg_dir)
                .expect("Failed to create user config directory for driver manager test");
            created = true;
        }

        let manifest_path = usercfg_dir.join("adbc-test-sqlite.toml");
        std::fs::write(&manifest_path, simple_manifest())
            .expect("Failed to write driver manager manifest to user config directory");

        // fail to load if the load flag doesn't have LOAD_FLAG_SEARCH_USER
        let err = ManagedDriver::load_from_name(
            "adbc-test-sqlite",
            None,
            AdbcVersion::V110,
            adbc_core::LOAD_FLAG_DEFAULT & !adbc_core::LOAD_FLAG_SEARCH_USER,
            None,
        )
        .unwrap_err();
        assert_eq!(err.status, Status::NotFound);

        // succeed loading if LOAD_FLAG_SEARCH_USER flag is set
        ManagedDriver::load_from_name(
            "adbc-test-sqlite",
            None,
            AdbcVersion::V110,
            adbc_core::LOAD_FLAG_SEARCH_USER,
            None,
        )
        .unwrap();

        std::fs::remove_file(&manifest_path)
            .expect("Failed to remove driver manager manifest from user config directory");
        if created {
            std::fs::remove_dir(usercfg_dir).unwrap();
        }
    }

    /// Regression test for https://github.com/apache/arrow-adbc/pull/3693
    /// Ensures driver manager tests for Windows pull in Windows crates. This
    /// can be removed/replace when more complete tests are added.
    #[test]
    #[cfg(target_os = "windows")]
    fn test_load_driver_from_registry() {
        use std::ffi::OsStr;
        let driver_name = OsStr::new("nonexistent_test_driver");
        let result =
            DriverLibrary::load_library_from_registry(windows_registry::CURRENT_USER, driver_name);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotFound);
    }

    #[test]
    fn test_parse_driver_uri() {
        let cases = vec![
            ("sqlite", Err(Status::InvalidArguments)),
            ("sqlite:", Ok(("sqlite", "sqlite:"))),
            ("sqlite:file::memory:", Ok(("sqlite", "file::memory:"))),
            (
                "sqlite:file::memory:?cache=shared",
                Ok(("sqlite", "file::memory:?cache=shared")),
            ),
            (
                "postgresql://a:b@localhost:9999/nonexistent",
                Ok(("postgresql", "postgresql://a:b@localhost:9999/nonexistent")),
            ),
        ];

        for (input, expected) in cases {
            let result = parse_driver_uri(input);
            match expected {
                Ok((exp_driver, exp_conn)) => {
                    let (driver, conn) = result.expect("Expected Ok result");
                    assert_eq!(driver, exp_driver);
                    assert_eq!(conn, exp_conn);
                }
                Err(exp_status) => {
                    let err = result.expect_err("Expected Err result");
                    assert_eq!(err.status, exp_status);
                }
            }
        }
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn test_parse_driver_uri_windows_file() {
        let tmp_dir = Builder::new()
            .prefix("adbc_tests")
            .tempdir()
            .expect("Failed to create temporary directory for driver manager manifest test");

        let temp_db_path = tmp_dir.path().join("test.dll");
        if let Some(parent) = temp_db_path.parent() {
            std::fs::create_dir_all(parent)
                .expect("Failed to create parent directory for manifest");
        }
        std::fs::write(&temp_db_path, b"").expect("Failed to create temporary database file");

        let (driver, conn) = parse_driver_uri(temp_db_path.to_str().unwrap()).unwrap();

        assert_eq!(driver, temp_db_path.to_str().unwrap());
        assert_eq!(conn, "");

        tmp_dir
            .close()
            .expect("Failed to close/remove temporary directory");
    }
}
