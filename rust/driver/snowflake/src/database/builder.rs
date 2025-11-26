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

//! A builder for a [`Database`]
//!
//!

use core::str;
#[cfg(feature = "env")]
use std::env;
use std::{fmt, path::PathBuf, str::FromStr, time::Duration};

use adbc_core::{
    error::{Error, Result, Status},
    options::{OptionDatabase, OptionValue},
    sync::Driver as _,
};
use url::{Host, Url};

use crate::{builder::BuilderIter, Database, Driver};
#[cfg(feature = "env")]
use crate::{
    builder::{env_parse, env_parse_map_err},
    duration::parse_duration,
};

/// Authentication types.
#[derive(Copy, Clone, Debug, Default)]
#[non_exhaustive]
pub enum AuthType {
    /// General username password authentication
    #[default]
    Snowflake,
    /// OAuth authentication
    OAuth,
    /// Use a browser to access an Fed and perform SSO authentication
    ExternalBrowser,
    /// Native okta URL to perform SSO authentication on Okta
    Okta,
    /// Use Jwt to perform authentication
    Jwt,
    /// Username and password with mfa
    UsernamePasswordMFA,
    /// Use a programmatic access token for authentication.
    Pat,
    /// Use Workload Identity Federation for authentication.
    Wif,
}

impl fmt::Display for AuthType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Snowflake => "auth_snowflake",
                Self::OAuth => "auth_oauth",
                Self::ExternalBrowser => "auth_ext_browser",
                Self::Okta => "auth_okta",
                Self::Jwt => "auth_jwt",
                Self::UsernamePasswordMFA => "auth_mfa",
                Self::Pat => "auth_pat",
                Self::Wif => "auth_wif",
            }
        )
    }
}

impl str::FromStr for AuthType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "auth_snowflake" => Ok(Self::Snowflake),
            "auth_oauth" => Ok(Self::OAuth),
            "auth_ext_browser" => Ok(Self::ExternalBrowser),
            "auth_okta" => Ok(Self::Okta),
            "auth_jwt" => Ok(Self::Jwt),
            "auth_mfa" => Ok(Self::UsernamePasswordMFA),
            "auth_pat" => Ok(Self::Pat),
            "auth_wif" => Ok(Self::Wif),
            _ => Err(Error::with_message_and_status(
                format!(
                    "invalid auth type: {s} (possible values: {}, {}, {}, {}, {}, {}, {}, {})",
                    Self::Snowflake,
                    Self::OAuth,
                    Self::ExternalBrowser,
                    Self::Okta,
                    Self::Jwt,
                    Self::UsernamePasswordMFA,
                    Self::Pat,
                    Self::Wif,
                ),
                Status::InvalidArguments,
            )),
        }
    }
}

/// Protocol types.
#[derive(Copy, Clone, Debug, Default)]
#[non_exhaustive]
pub enum Protocol {
    /// Use `https`.
    #[default]
    Https,
    /// Use `http`.
    Http,
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Https => "https",
                Self::Http => "http",
            }
        )
    }
}

impl str::FromStr for Protocol {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "https" | "HTTPS" => Ok(Self::Https),
            "http" | "HTTP" => Ok(Self::Http),
            _ => Err(Error::with_message_and_status(
                format!(
                    "invalid protocol type: {s} (possible values: {}, {})",
                    Self::Https,
                    Self::Http
                ),
                Status::InvalidArguments,
            )),
        }
    }
}

/// Log levels.
#[derive(Copy, Clone, Debug, Default)]
#[non_exhaustive]
pub enum LogLevel {
    /// Trace level
    Trace,
    /// Debug level
    Debug,
    /// Info level
    Info,
    /// Warn level
    Warn,
    /// Error level
    Error,
    /// Fatal level
    Fatal,
    /// Off
    #[default]
    Off,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Trace => "trace",
                Self::Debug => "debug",
                Self::Info => "info",
                Self::Warn => "warn",
                Self::Error => "error",
                Self::Fatal => "fatal",
                Self::Off => "off",
            }
        )
    }
}

impl str::FromStr for LogLevel {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "trace" => Ok(Self::Trace),
            "debug" => Ok(Self::Debug),
            "info" => Ok(Self::Info),
            "warn" => Ok(Self::Warn),
            "error" => Ok(Self::Error),
            "fatal" => Ok(Self::Fatal),
            "off" => Ok(Self::Off),
            _ => Err(Error::with_message_and_status(
                format!(
                    "invalid log level: {s} (possible values: {}, {}, {}, {}, {}, {}, {})",
                    Self::Trace,
                    Self::Debug,
                    Self::Info,
                    Self::Warn,
                    Self::Error,
                    Self::Fatal,
                    Self::Off
                ),
                Status::InvalidArguments,
            )),
        }
    }
}

/// A builder for [`Database`].
///
/// The builder can be used to initialize a [`Database`] with
/// [`Builder::build`] or by directly passing it to
/// [`Driver::new_database_with_opts`].
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct Builder {
    /// The URI ([`OptionDatabase::Uri`]).
    pub uri: Option<Url>,

    /// The username ([`OptionDatabase::Username`]).
    pub username: Option<String>,

    /// The password ([`OptionDatabase::Password`]).
    pub password: Option<String>,

    /// The name of the database ([`Self::DATABASE`]).
    pub database: Option<String>,

    /// The name of the schema ([`Self::SCHEMA`]).
    pub schema: Option<String>,

    /// The name of the warehouse ([`Self::WAREHOUSE`]).
    pub warehouse: Option<String>,

    /// The name of the role ([`Self::ROLE`]).
    pub role: Option<String>,

    /// The name of the region ([`Self::REGION`]).
    pub region: Option<String>,

    /// The name of the account ([`Self::ACCOUNT`]).
    pub account: Option<String>,

    /// The protocol ([`Self::PROTOCOL`]).
    pub protocol: Option<Protocol>,

    /// The port ([`Self::PORT`]).
    pub port: Option<u16>,

    /// The host ([`Self::HOST`]).
    pub host: Option<Host>,

    /// The authentication type ([`Self::AUTH_TYPE`]).
    pub auth_type: Option<AuthType>,

    /// Login timeout duration ([`Self::LOGIN_TIMEOUT`]).
    pub login_timeout: Option<Duration>,

    /// Request timeout duration ([`Self::REQUEST_TIMEOUT`]).
    pub request_timeout: Option<Duration>,

    /// JWT expire timeout duration ([`Self::JWT_EXPIRE_TIMEOUT`]).
    pub jwt_expire_timeout: Option<Duration>,

    /// Client timeout duration ([`Self::CLIENT_TIMEOUT`]).
    pub client_timeout: Option<Duration>,

    /// Use high precision ([`Self::USE_HIGH_PRECISION`]).
    pub use_high_precision: Option<bool>,

    /// Application name ([`Self::APPLICATION_NAME`]).
    pub application_name: Option<String>,

    /// SSL skip verify ([`Self::SSL_SKIP_VERIFY`]).
    pub ssl_skip_verify: Option<bool>,

    /// OCSP fail open mode ([`Self::OCSP_FAIL_OPEN_MODE`]).
    pub ocsp_fail_open_mode: Option<bool>,

    /// Auth token ([`Self::AUTH_TOKEN`]).
    pub auth_token: Option<String>,

    /// Auth Okta url ([`Self::AUTH_OKTA_URL`]).
    pub auth_okta_url: Option<Url>,

    /// Keep session alive ([`Self::KEEP_SESSION_ALIVE`]).
    pub keep_session_alive: Option<bool>,

    /// JWT private key file path ([`Self::JWT_PRIVATE_KEY`]).
    pub jwt_private_key: Option<PathBuf>,

    /// JWT private key PKCS 8 value ([`Self::JWT_PRIVATE_KEY_PKCS8_VALUE`]).
    pub jwt_private_key_pkcs8_value: Option<String>,

    /// JWT private key PKCS 8 password ([`Self::JWT_PRIVATE_KEY_PKCS8_PASSWORD`]).
    pub jwt_private_key_pkcs8_password: Option<String>,

    /// Disable telemetry ([`Self::DISABLE_TELEMETRY`]).
    pub disable_telemetry: Option<bool>,

    /// Log tracing level ([`Self::LOG_TRACING`]).
    pub log_tracing: Option<LogLevel>,

    /// Client config file path ([`Self::CLIENT_CONFIG_FILE`]).
    pub client_config_file: Option<PathBuf>,

    /// Client cache MFA token ([`Self::CLIENT_CACHE_MFA_TOKEN`]).
    pub client_cache_mfa_token: Option<bool>,

    /// Client store temporary credentials ([`Self::CLIENT_STORE_TEMP_CREDS`]).
    pub client_store_temp_creds: Option<bool>,

    /// When using [`AuthType::Wif`] for workload identity federation authentication, this
    /// must be set to the appropriate identity provider. ([`Self::CLIENT_IDENTITY_PROVIDER`]).
    pub client_identity_provider: Option<String>,

    /// Other options.
    pub other: Vec<(OptionDatabase, OptionValue)>,
}

impl fmt::Debug for Builder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const HIDDEN: &str = "*****";
        f.debug_struct("Builder")
            .field(
                "uri",
                &self.uri.clone().map(|mut url| {
                    let _ = url.set_password(url.password().and(Some(HIDDEN)));
                    url
                }),
            )
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| HIDDEN))
            .field("database", &self.database)
            .field("schema", &self.schema)
            .field("warehouse", &self.warehouse)
            .field("role", &self.role)
            .field("region", &self.region)
            .field("account", &self.account)
            .field("protocol", &self.protocol)
            .field("port", &self.port)
            .field("host", &self.host)
            .field("auth_type", &self.auth_type)
            .field("login_timeout", &self.login_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("jwt_expire_timeout", &self.jwt_expire_timeout)
            .field("client_timeout", &self.client_timeout)
            .field("use_high_precision", &self.use_high_precision)
            .field("application_name", &self.application_name)
            .field("ssl_skip_verify", &self.ssl_skip_verify)
            .field("ocsp_fail_open_mode", &self.ocsp_fail_open_mode)
            .field("auth_token", &self.auth_token.as_ref().map(|_| HIDDEN))
            .field("auth_okta_url", &self.auth_okta_url)
            .field("keep_session_alive", &self.keep_session_alive)
            .field("jwt_private_key", &self.jwt_private_key)
            .field(
                "jwt_private_key_pkcs8_value",
                &self.jwt_private_key_pkcs8_value.as_ref().map(|_| HIDDEN),
            )
            .field(
                "jwt_private_key_pkcs8_password",
                &self.jwt_private_key_pkcs8_password.as_ref().map(|_| HIDDEN),
            )
            .field("disable_telemetry", &self.disable_telemetry)
            .field("log_tracing", &self.log_tracing)
            .field("client_config_file", &self.client_config_file)
            .field("client_cache_mfa_token", &self.client_cache_mfa_token)
            .field("client_store_temp_creds", &self.client_store_temp_creds)
            .field("client_identity_provider", &self.client_identity_provider)
            .field("...", &self.other)
            .finish()
    }
}

#[cfg(feature = "env")]
impl Builder {
    /// See [`Self::uri`].
    pub const URI_ENV: &str = "ADBC_SNOWFLAKE_URI";

    /// See [`Self::username`].
    pub const USERNAME_ENV: &str = "ADBC_SNOWFLAKE_USERNAME";

    /// See [`Self::password`].
    pub const PASSWORD_ENV: &str = "ADBC_SNOWFLAKE_PASSWORD";

    /// See [`Self::database`].
    pub const DATABASE_ENV: &str = "ADBC_SNOWFLAKE_SQL_DB";

    /// See [`Self::schema`].
    pub const SCHEMA_ENV: &str = "ADBC_SNOWFLAKE_SQL_SCHEMA";

    /// See [`Self::warehouse`].
    pub const WAREHOUSE_ENV: &str = "ADBC_SNOWFLAKE_SQL_WAREHOUSE";

    /// See [`Self::role`].
    pub const ROLE_ENV: &str = "ADBC_SNOWFLAKE_SQL_ROLE";

    /// See [`Self::region`].
    pub const REGION_ENV: &str = "ADBC_SNOWFLAKE_SQL_REGION";

    /// See [`Self::account`].
    pub const ACCOUNT_ENV: &str = "ADBC_SNOWFLAKE_SQL_ACCOUNT";

    /// See [`Self::protocol`].
    pub const PROTOCOL_ENV: &str = "ADBC_SNOWFLAKE_SQL_URI_PROTOCOL";

    /// See [`Self::port`].
    pub const PORT_ENV: &str = "ADBC_SNOWFLAKE_SQL_URI_PORT";

    /// See [`Self::host`].
    pub const HOST_ENV: &str = "ADBC_SNOWFLAKE_SQL_URI_HOST";

    /// See [`Self::auth_type`].
    pub const AUTH_TYPE_ENV: &str = "ADBC_SNOWFLAKE_SQL_AUTH_TYPE";

    /// See [`Self::login_timeout`].
    pub const LOGIN_TIMEOUT_ENV: &str = "ADBC_SNOWFLAKE_SQL_CLIENT_OPTIONS_LOGIN_TIMEOUT";

    /// See [`Self::request_timeout`].
    pub const REQUEST_TIMEOUT_ENV: &str = "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_REQUEST_TIMEOUT";

    /// See [`Self::jwt_expire_timeout`].
    pub const JWT_EXPIRE_TIMEOUT_ENV: &str = "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_EXPIRE_TIMEOUT";

    /// See [`Self::client_timeout`].
    pub const CLIENT_TIMEOUT_ENV: &str = "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_CLIENT_TIMEOUT";

    /// See [`Self::use_high_precision`].
    pub const USE_HIGH_PRECISION_ENV: &str = "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_USE_HIGH_PRECISION";

    /// See [`Self::application_name`].
    pub const APPLICATION_NAME_ENV: &str = "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_APP_NAME";

    /// See [`Self::ssl_skip_verify`].
    pub const SSL_SKIP_VERIFY_ENV: &str = "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_TLS_SKIP_VERIFY";

    /// See [`Self::ocsp_fail_open_mode`].
    pub const OCSP_FAIL_OPEN_MODE_ENV: &str =
        "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_OCSP_FAIL_OPEN_MODE";

    /// See [`Self::auth_token`].
    pub const AUTH_TOKEN_ENV: &str = "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_AUTH_TOKEN";

    /// See [`Self::auth_okta_url`].
    pub const AUTH_OKTA_URL_ENV: &str = "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_OKTA_URL";

    /// See [`Self::keep_session_alive`].
    pub const KEEP_SESSION_ALIVE_ENV: &str = "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_KEEP_SESSION_ALIVE";

    /// See [`Self::jwt_private_key`].
    pub const JWT_PRIVATE_KEY_ENV: &str = "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY";

    /// See [`Self::jwt_private_key_pkcs8_value`].
    pub const JWT_PRIVATE_KEY_PKCS8_VALUE_ENV: &str =
        "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY_PKCS8_VALUE";

    /// See [`Self::jwt_private_key_pkcs8_password`].
    pub const JWT_PRIVATE_KEY_PKCS8_PASSWORD_ENV: &str =
        "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_JWT_PRIVATE_KEY_PKCS8_PASSWORD";

    /// See [`Self::disable_telemetry`].
    pub const DISABLE_TELEMETRY_ENV: &str = "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_DISABLE_TELEMETRY";

    /// See [`Self::log_tracing`].
    pub const LOG_TRACING_ENV: &str = "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_TRACING";

    /// See [`Self::client_config_file`].
    pub const CLIENT_CONFIG_FILE_ENV: &str = "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_CONFIG_FILE";

    /// See [`Self::client_cache_mfa_token`].
    pub const CLIENT_CACHE_MFA_TOKEN_ENV: &str = "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_CACHE_MFA_TOKEN";

    /// See [`Self::client_store_temp_creds`].
    pub const CLIENT_STORE_TEMP_CREDS_ENV: &str =
        "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_STORE_TEMP_CREDS";

    /// See [`Self::client_identity_provider`]
    pub const CLIENT_IDENTITY_PROVIDER_ENV: &str =
        "ADBC_SNOWFLAKE_SQL_CLIENT_OPTION_IDENTITY_PROVIDER";

    /// Construct a builder, setting values based on values of the
    /// configuration environment variables.
    ///
    /// # Error
    ///
    /// Returns an error when environment variables are set but their values
    /// fail to parse.
    pub fn from_env() -> Result<Self> {
        #[cfg(feature = "dotenv")]
        let _ = dotenvy::dotenv();

        let uri = env_parse_map_err(Self::URI_ENV, Url::parse)?;
        let username = env::var(Self::USERNAME_ENV).ok();
        let password = env::var(Self::PASSWORD_ENV).ok();
        let database = env::var(Self::DATABASE_ENV).ok();
        let schema = env::var(Self::SCHEMA_ENV).ok();
        let warehouse = env::var(Self::WAREHOUSE_ENV).ok();
        let role = env::var(Self::ROLE_ENV).ok();
        let region = env::var(Self::REGION_ENV).ok();
        let account = env::var(Self::ACCOUNT_ENV).ok();
        let protocol = env_parse(Self::PROTOCOL_ENV, str::parse)?;
        let port = env_parse_map_err(Self::PORT_ENV, str::parse)?;
        let host = env_parse_map_err(Self::HOST_ENV, Host::parse)?;
        let auth_type = env_parse(Self::AUTH_TYPE_ENV, str::parse)?;
        let login_timeout = env_parse(Self::LOGIN_TIMEOUT_ENV, parse_duration)?;
        let request_timeout = env_parse(Self::REQUEST_TIMEOUT_ENV, parse_duration)?;
        let jwt_expire_timeout = env_parse(Self::JWT_EXPIRE_TIMEOUT_ENV, parse_duration)?;
        let client_timeout = env_parse(Self::CLIENT_TIMEOUT_ENV, parse_duration)?;
        let use_high_precision = env_parse_map_err(Self::USE_HIGH_PRECISION_ENV, str::parse)?;
        let application_name = env::var(Self::APPLICATION_NAME_ENV).ok();
        let ssl_skip_verify = env_parse_map_err(Self::SSL_SKIP_VERIFY_ENV, str::parse)?;
        let ocsp_fail_open_mode = env_parse_map_err(Self::OCSP_FAIL_OPEN_MODE_ENV, str::parse)?;
        let auth_token = env::var(Self::AUTH_TOKEN_ENV).ok();
        let auth_okta_url = env_parse_map_err(Self::AUTH_OKTA_URL_ENV, Url::parse)?;
        let keep_session_alive = env_parse_map_err(Self::OCSP_FAIL_OPEN_MODE_ENV, str::parse)?;
        let jwt_private_key = env_parse_map_err(Self::JWT_PRIVATE_KEY_ENV, str::parse)?;
        let jwt_private_key_pkcs8_value = env::var(Self::JWT_PRIVATE_KEY_PKCS8_VALUE_ENV).ok();
        let jwt_private_key_pkcs8_password =
            env::var(Self::JWT_PRIVATE_KEY_PKCS8_PASSWORD_ENV).ok();
        let disable_telemetry = env_parse_map_err(Self::DISABLE_TELEMETRY_ENV, str::parse)?;
        let log_tracing = env_parse(Self::LOG_TRACING_ENV, str::parse)?;
        let client_config_file = env_parse_map_err(Self::CLIENT_CONFIG_FILE_ENV, str::parse)?;
        let client_cache_mfa_token =
            env_parse_map_err(Self::CLIENT_CACHE_MFA_TOKEN_ENV, str::parse)?;
        let client_store_temp_creds =
            env_parse_map_err(Self::CLIENT_STORE_TEMP_CREDS_ENV, str::parse)?;
        let client_identity_provider =
            env_parse_map_err(Self::CLIENT_IDENTITY_PROVIDER_ENV, str::parse)?;

        Ok(Self {
            uri,
            username,
            password,
            database,
            schema,
            warehouse,
            role,
            region,
            account,
            protocol,
            port,
            host,
            auth_type,
            login_timeout,
            request_timeout,
            jwt_expire_timeout,
            client_timeout,
            use_high_precision,
            application_name,
            ssl_skip_verify,
            ocsp_fail_open_mode,
            auth_token,
            auth_okta_url,
            keep_session_alive,
            jwt_private_key,
            jwt_private_key_pkcs8_value,
            jwt_private_key_pkcs8_password,
            disable_telemetry,
            log_tracing,
            client_config_file,
            client_cache_mfa_token,
            client_store_temp_creds,
            client_identity_provider,
            ..Default::default()
        })
    }
}

impl Builder {
    /// Number of fields in the builder (except other).
    const COUNT: usize = 33;

    pub const DATABASE: &str = "adbc.snowflake.sql.db";
    pub const SCHEMA: &str = "adbc.snowflake.sql.schema";
    pub const WAREHOUSE: &str = "adbc.snowflake.sql.warehouse";
    pub const ROLE: &str = "adbc.snowflake.sql.role";
    pub const REGION: &str = "adbc.snowflake.sql.region";
    pub const ACCOUNT: &str = "adbc.snowflake.sql.account";
    pub const PROTOCOL: &str = "adbc.snowflake.sql.uri.protocol";
    pub const PORT: &str = "adbc.snowflake.sql.uri.port";
    pub const HOST: &str = "adbc.snowflake.sql.uri.host";
    pub const AUTH_TYPE: &str = "adbc.snowflake.sql.auth_type";
    pub const LOGIN_TIMEOUT: &str = "adbc.snowflake.sql.client_option.login_timeout";
    pub const REQUEST_TIMEOUT: &str = "adbc.snowflake.sql.client_option.request_timeout";
    pub const JWT_EXPIRE_TIMEOUT: &str = "adbc.snowflake.sql.client_option.jwt_expire_timeout";
    pub const CLIENT_TIMEOUT: &str = "adbc.snowflake.sql.client_option.client_timeout";
    pub const USE_HIGH_PRECISION: &str = "adbc.snowflake.sql.client_option.use_high_precision";
    pub const APPLICATION_NAME: &str = "adbc.snowflake.sql.client_option.app_name";
    pub const SSL_SKIP_VERIFY: &str = "adbc.snowflake.sql.client_option.tls_skip_verify";
    pub const OCSP_FAIL_OPEN_MODE: &str = "adbc.snowflake.sql.client_option.ocsp_fail_open_mode";
    pub const AUTH_TOKEN: &str = "adbc.snowflake.sql.client_option.auth_token";
    pub const AUTH_OKTA_URL: &str = "adbc.snowflake.sql.client_option.okta_url";
    pub const KEEP_SESSION_ALIVE: &str = "adbc.snowflake.sql.client_option.keep_session_alive";
    pub const JWT_PRIVATE_KEY: &str = "adbc.snowflake.sql.client_option.jwt_private_key";
    pub const JWT_PRIVATE_KEY_PKCS8_VALUE: &str =
        "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_value";
    pub const JWT_PRIVATE_KEY_PKCS8_PASSWORD: &str =
        "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_password";
    pub const DISABLE_TELEMETRY: &str = "adbc.snowflake.sql.client_option.disable_telemetry";
    pub const LOG_TRACING: &str = "adbc.snowflake.sql.client_option.tracing";
    pub const CLIENT_CONFIG_FILE: &str = "adbc.snowflake.sql.client_option.config_file";
    pub const CLIENT_CACHE_MFA_TOKEN: &str = "adbc.snowflake.sql.client_option.cache_mfa_token";
    pub const CLIENT_STORE_TEMP_CREDS: &str = "adbc.snowflake.sql.client_option.store_temp_creds";
    pub const CLIENT_IDENTITY_PROVIDER: &str = "adbc.snowflake.sql.client_option.identity_provider";

    /// Use the provided URI ([`Self::uri`]).
    pub fn with_uri(mut self, uri: Url) -> Self {
        self.uri = Some(uri);
        self
    }

    /// Parse and use the provided URI ([`Self::uri`]).
    ///
    /// Returns an error when parsing fails.
    pub fn with_parse_uri(self, uri: impl AsRef<str>) -> Result<Self> {
        Url::parse(uri.as_ref())
            .map_err(|error| {
                Error::with_message_and_status(error.to_string(), Status::InvalidArguments)
            })
            .map(|uri| self.with_uri(uri))
    }

    /// Use the provided username ([`Self::username`]).
    pub fn with_username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }

    /// Use the provided password ([`Self::password`]).
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Use the provided database ([`Self::database`]).
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    /// Use the provided schema ([`Self::schema`]).
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Use the provided warehouse ([`Self::warehouse`]).
    pub fn with_warehouse(mut self, warehouse: impl Into<String>) -> Self {
        self.warehouse = Some(warehouse.into());
        self
    }

    /// Use the provided role ([`Self::role`]).
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.role = Some(role.into());
        self
    }

    /// Use the provided region ([`Self::region`]).
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Use the provided account ([`Self::account`]).
    pub fn with_account(mut self, account: impl Into<String>) -> Self {
        self.account = Some(account.into());
        self
    }

    /// Use the provided protocol ([`Self::protocol`]).
    pub fn with_protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = Some(protocol);
        self
    }

    /// Parse and use the provided protocol ([`Self::protocol`]).
    ///
    /// Returns an error when parsing fails.
    pub fn with_parse_protocol(self, protocol: impl AsRef<str>) -> Result<Self> {
        Protocol::from_str(protocol.as_ref()).map(|protocol| self.with_protocol(protocol))
    }

    /// Use the provided port ([`Self::port`]).
    ///
    /// # Example
    ///
    /// ```
    /// # use adbc_snowflake::database::Builder;
    /// let builder = Builder::default().with_port(443);
    ///
    /// assert_eq!(builder.port, Some(443));
    /// ```
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    /// Use the provided host ([`Self::host`]).
    pub fn with_host(mut self, host: Host) -> Self {
        self.host = Some(host);
        self
    }

    /// Parse and use the provided host ([`Self::host`]).
    ///
    /// Returns an error when parsing fails.
    pub fn with_parse_host(self, host: impl AsRef<str>) -> Result<Self> {
        Host::parse(host.as_ref())
            .map_err(|error| {
                Error::with_message_and_status(error.to_string(), Status::InvalidArguments)
            })
            .map(|host| self.with_host(host))
    }

    /// Use the provided auth type ([`Self::auth_type`]).
    pub fn with_auth_type(mut self, auth_type: AuthType) -> Self {
        self.auth_type = Some(auth_type);
        self
    }

    /// Parse and use the provided auth type ([`Self::auth_type`]).
    ///
    /// Returns an error when parsing fails.
    pub fn with_parse_auth_type(self, auth_type: impl AsRef<str>) -> Result<Self> {
        AuthType::from_str(auth_type.as_ref()).map(|auth_type| self.with_auth_type(auth_type))
    }

    /// Use the provided login timeout ([`Self::login_timeout`]).
    pub fn with_login_timeout(mut self, login_timeout: Duration) -> Self {
        self.login_timeout = Some(login_timeout);
        self
    }

    /// Parse and use the provided login timeout ([`Self::login_timeout`]).
    ///
    /// Returns an error when parsing fails.
    #[cfg(feature = "env")]
    pub fn with_parse_login_timeout(self, login_timeout: impl AsRef<str>) -> Result<Self> {
        parse_duration(login_timeout.as_ref())
            .map(|login_timeout| self.with_login_timeout(login_timeout))
    }

    /// Use the provided request timeout ([`Self::request_timeout`]).
    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = Some(request_timeout);
        self
    }

    /// Parse and use the provided request timeout ([`Self::request_timeout`]).
    ///
    /// Returns an error when parsing fails.
    #[cfg(feature = "env")]
    pub fn with_parse_request_timeout(self, request_timeout: impl AsRef<str>) -> Result<Self> {
        parse_duration(request_timeout.as_ref())
            .map(|request_timeout| self.with_request_timeout(request_timeout))
    }

    /// Use the provided JWT expire timeout ([`Self::jwt_expire_timeout`]).
    pub fn with_jwt_expire_timeout(mut self, jwt_expire_timeout: Duration) -> Self {
        self.jwt_expire_timeout = Some(jwt_expire_timeout);
        self
    }

    /// Parse and use the provided JWT expire timeout ([`Self::jwt_expire_timeout`]).
    ///
    /// Returns an error when parsing fails.
    #[cfg(feature = "env")]
    pub fn with_parse_jwt_expire_timeout(
        self,
        jwt_expire_timeout: impl AsRef<str>,
    ) -> Result<Self> {
        parse_duration(jwt_expire_timeout.as_ref())
            .map(|jwt_expire_timeout| self.with_jwt_expire_timeout(jwt_expire_timeout))
    }

    /// Use the provided client timeout ([`Self::client_timeout`]).
    pub fn with_client_timeout(mut self, client_timeout: Duration) -> Self {
        self.client_timeout = Some(client_timeout);
        self
    }

    /// Parse and use the provided client timeout ([`Self::client_timeout`]).
    ///
    /// Returns an error when parsing fails.
    #[cfg(feature = "env")]
    pub fn with_parse_client_timeout(self, client_timeout: impl AsRef<str>) -> Result<Self> {
        parse_duration(client_timeout.as_ref())
            .map(|client_timeout| self.with_client_timeout(client_timeout))
    }

    /// Use high precision ([`Self::use_high_precision`]).
    pub fn with_high_precision(mut self, use_high_precision: bool) -> Self {
        self.use_high_precision = Some(use_high_precision);
        self
    }

    /// Use the provided application name ([`Self::application_name`]).
    pub fn with_application_name(mut self, application_name: impl Into<String>) -> Self {
        self.application_name = Some(application_name.into());
        self
    }

    /// Use SSL skip verify ([`Self::ssl_skip_verify`]).
    pub fn with_ssl_skip_verify(mut self, ssl_skip_verify: bool) -> Self {
        self.ssl_skip_verify = Some(ssl_skip_verify);
        self
    }

    /// Use OCSP fail open mode ([`Self::ocsp_fail_open_mode`]).
    pub fn with_ocsp_fail_open_mode(mut self, ocsp_fail_open_mode: bool) -> Self {
        self.ocsp_fail_open_mode = Some(ocsp_fail_open_mode);
        self
    }

    /// Use the provided auth token ([`Self::auth_token`]).
    pub fn with_auth_token(mut self, auth_token: impl Into<String>) -> Self {
        self.auth_token = Some(auth_token.into());
        self
    }

    /// Use the provided auth Okta URL ([`Self::auth_okta_url`]).
    pub fn with_auth_okta_url(mut self, auth_okta_url: Url) -> Self {
        self.auth_okta_url = Some(auth_okta_url);
        self
    }

    /// Parse and use the provided auth Okta URL ([`Self::auth_okta_url`]).
    ///
    /// Returns an error when parsing fails.
    pub fn with_parse_auth_okta_url(self, auth_okta_url: impl AsRef<str>) -> Result<Self> {
        Url::parse(auth_okta_url.as_ref())
            .map_err(|error| {
                Error::with_message_and_status(error.to_string(), Status::InvalidArguments)
            })
            .map(|auth_okta_url| self.with_auth_okta_url(auth_okta_url))
    }

    /// Use keep session alive ([`Self::keep_session_alive`]).
    pub fn with_keep_session_alive(mut self, keep_session_alive: bool) -> Self {
        self.keep_session_alive = Some(keep_session_alive);
        self
    }

    /// Use the provided JWT private key path ([`Self::jwt_private_key`]).
    pub fn with_jwt_private_key(mut self, jwt_private_key: PathBuf) -> Self {
        self.jwt_private_key = Some(jwt_private_key);
        self
    }

    /// Use the provided JWT private key PKCS 8 value ([`Self::jwt_private_key_pkcs8_value`]).
    pub fn with_jwt_private_key_pkcs8_value(mut self, jwt_private_key_pkcs8_value: String) -> Self {
        self.jwt_private_key_pkcs8_value = Some(jwt_private_key_pkcs8_value);
        self
    }

    /// Use the provided JWT private key PKCS 8 password ([`Self::jwt_private_key_pkcs8_password`]).
    pub fn with_jwt_private_key_pkcs8_password(
        mut self,
        jwt_private_key_pkcs8_password: String,
    ) -> Self {
        self.jwt_private_key_pkcs8_password = Some(jwt_private_key_pkcs8_password);
        self
    }

    /// Use disable telemetry ([`Self::disable_telemetry`]).
    pub fn with_disable_telemetry(mut self, disable_telemetry: bool) -> Self {
        self.disable_telemetry = Some(disable_telemetry);
        self
    }

    /// Use the provided tracing log level ([`Self::log_tracing`]).
    pub fn with_log_tracing(mut self, log_tracing: LogLevel) -> Self {
        self.log_tracing = Some(log_tracing);
        self
    }

    /// Use the provided client config file path ([`Self::client_config_file`]).
    pub fn with_client_config_file(mut self, client_config_file: PathBuf) -> Self {
        self.client_config_file = Some(client_config_file);
        self
    }

    /// Use cache MFA token ([`Self::client_cache_mfa_token`]).
    pub fn with_client_cache_mfa_token(mut self, client_cache_mfa_token: bool) -> Self {
        self.client_cache_mfa_token = Some(client_cache_mfa_token);
        self
    }

    /// Use store temporary credentials ([`Self::client_store_temp_creds`]).
    pub fn with_client_store_temp_creds(mut self, client_store_temp_creds: bool) -> Self {
        self.client_store_temp_creds = Some(client_store_temp_creds);
        self
    }

    /// Use the provided client identity provider ([`Self::client_identity_provider`]).
    pub fn with_client_identity_provider(mut self, client_identity_provider: String) -> Self {
        self.client_identity_provider = Some(client_identity_provider);
        self
    }
}

impl Builder {
    /// Attempt to initialize a [`Database`] using the values provided to this
    /// builder using the provided [`Driver`].
    pub fn build(self, driver: &mut Driver) -> Result<Database> {
        driver.new_database_with_opts(self)
    }
}

impl IntoIterator for Builder {
    type Item = (OptionDatabase, OptionValue);
    type IntoIter = BuilderIter<OptionDatabase, { Self::COUNT }>;

    fn into_iter(self) -> Self::IntoIter {
        BuilderIter::new(
            [
                self.uri
                    .as_ref()
                    .map(ToString::to_string)
                    .map(OptionValue::String)
                    .map(|value| (OptionDatabase::Uri, value)),
                self.username
                    .map(OptionValue::String)
                    .map(|value| (OptionDatabase::Username, value)),
                self.password
                    .map(OptionValue::String)
                    .map(|value| (OptionDatabase::Password, value)),
                self.database
                    .map(OptionValue::String)
                    .map(|value| (Builder::DATABASE.into(), value)),
                self.schema
                    .map(OptionValue::String)
                    .map(|value| (Builder::SCHEMA.into(), value)),
                self.warehouse
                    .map(OptionValue::String)
                    .map(|value| (Builder::WAREHOUSE.into(), value)),
                self.role
                    .map(OptionValue::String)
                    .map(|value| (Builder::ROLE.into(), value)),
                self.region
                    .map(OptionValue::String)
                    .map(|value| (Builder::REGION.into(), value)),
                self.account
                    .map(OptionValue::String)
                    .map(|value| (Builder::ACCOUNT.into(), value)),
                self.protocol
                    .as_ref()
                    .map(ToString::to_string)
                    .map(OptionValue::String)
                    .map(|value| (Builder::PROTOCOL.into(), value)),
                self.port
                    .as_ref()
                    // TODO(mbrobbel): the driver expects a string here?
                    // .map(i64::from)
                    // .map(OptionValue::Int)
                    .map(ToString::to_string)
                    .map(OptionValue::String)
                    .map(|value| (Builder::PORT.into(), value)),
                self.host
                    .as_ref()
                    .map(ToString::to_string)
                    .map(OptionValue::String)
                    .map(|value| (Builder::HOST.into(), value)),
                self.auth_type
                    .as_ref()
                    .map(ToString::to_string)
                    .map(OptionValue::String)
                    .map(|value| (Builder::AUTH_TYPE.into(), value)),
                self.login_timeout
                    .as_ref()
                    .map(|duration| format!("{duration:?}"))
                    .map(OptionValue::String)
                    .map(|value| (Builder::LOGIN_TIMEOUT.into(), value)),
                self.request_timeout
                    .as_ref()
                    .map(|duration| format!("{duration:?}"))
                    .map(OptionValue::String)
                    .map(|value| (Builder::REQUEST_TIMEOUT.into(), value)),
                self.jwt_expire_timeout
                    .as_ref()
                    .map(|duration| format!("{duration:?}"))
                    .map(OptionValue::String)
                    .map(|value| (Builder::JWT_EXPIRE_TIMEOUT.into(), value)),
                self.client_timeout
                    .as_ref()
                    .map(|duration| format!("{duration:?}"))
                    .map(OptionValue::String)
                    .map(|value| (Builder::CLIENT_TIMEOUT.into(), value)),
                self.use_high_precision
                    .as_ref()
                    .map(ToString::to_string)
                    .map(OptionValue::String)
                    .map(|value| (Builder::USE_HIGH_PRECISION.into(), value)),
                self.application_name
                    .map(OptionValue::String)
                    .map(|value| (Builder::APPLICATION_NAME.into(), value)),
                self.ssl_skip_verify
                    .as_ref()
                    .map(ToString::to_string)
                    .map(OptionValue::String)
                    .map(|value| (Builder::SSL_SKIP_VERIFY.into(), value)),
                self.ocsp_fail_open_mode
                    .as_ref()
                    .map(ToString::to_string)
                    .map(OptionValue::String)
                    .map(|value| (Builder::OCSP_FAIL_OPEN_MODE.into(), value)),
                self.auth_token
                    .map(OptionValue::String)
                    .map(|value| (Builder::AUTH_TOKEN.into(), value)),
                self.auth_okta_url
                    .as_ref()
                    .map(ToString::to_string)
                    .map(OptionValue::String)
                    .map(|value| (Builder::AUTH_OKTA_URL.into(), value)),
                self.keep_session_alive
                    .as_ref()
                    .map(ToString::to_string)
                    .map(OptionValue::String)
                    .map(|value| (Builder::KEEP_SESSION_ALIVE.into(), value)),
                self.jwt_private_key
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .map(OptionValue::String)
                    .map(|value| (Builder::JWT_PRIVATE_KEY.into(), value)),
                self.jwt_private_key_pkcs8_value
                    .as_ref()
                    .map(ToString::to_string)
                    .map(OptionValue::String)
                    .map(|value| (Builder::JWT_PRIVATE_KEY_PKCS8_VALUE.into(), value)),
                self.jwt_private_key_pkcs8_password
                    .as_ref()
                    .map(ToString::to_string)
                    .map(OptionValue::String)
                    .map(|value| (Builder::JWT_PRIVATE_KEY_PKCS8_PASSWORD.into(), value)),
                self.disable_telemetry
                    .as_ref()
                    .map(ToString::to_string)
                    .map(OptionValue::String)
                    .map(|value| (Builder::DISABLE_TELEMETRY.into(), value)),
                self.log_tracing
                    .as_ref()
                    .map(ToString::to_string)
                    .map(OptionValue::String)
                    .map(|value| (Builder::LOG_TRACING.into(), value)),
                self.client_config_file
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .map(OptionValue::String)
                    .map(|value| (Builder::CLIENT_CONFIG_FILE.into(), value)),
                self.client_cache_mfa_token
                    .as_ref()
                    .map(ToString::to_string)
                    .map(OptionValue::String)
                    .map(|value| (Builder::CLIENT_CACHE_MFA_TOKEN.into(), value)),
                self.client_store_temp_creds
                    .as_ref()
                    .map(ToString::to_string)
                    .map(OptionValue::String)
                    .map(|value| (Builder::CLIENT_STORE_TEMP_CREDS.into(), value)),
                self.client_identity_provider
                    .as_ref()
                    .map(ToString::to_string)
                    .map(OptionValue::String)
                    .map(|value| (Builder::CLIENT_IDENTITY_PROVIDER.into(), value)),
            ],
            self.other,
        )
    }
}

#[cfg(test)]
#[cfg(feature = "env")]
mod tests {
    use std::env;

    use adbc_core::error::Status;

    use super::*;

    #[test]
    fn from_env_parse_error() {
        // Set a value that fails to parse to a LogLevel
        env::set_var(Builder::LOG_TRACING_ENV, "warning");
        let result = Builder::from_env();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            Error::with_message_and_status(
                "invalid log level: warning (possible values: trace, debug, info, warn, error, fatal, off)",
                Status::InvalidArguments
            )
        );
        // Fix it to move on
        env::set_var(Builder::LOG_TRACING_ENV, "warn");

        // Set a value that fails to parse to a duration
        env::set_var(Builder::LOGIN_TIMEOUT_ENV, "forever");
        let result = Builder::from_env();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            Error::with_message_and_status("invalid duration (valid durations are a sequence of decimal numbers, each with optional fraction and a unit suffix, such as 300ms, 1.5h, 2h45m, valid time units are ns, us, ms, s, m, h)", Status::InvalidArguments)
        );
    }
}
