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

#pragma once

#include <memory>
#include <string>

#include <arrow-adbc/adbc.h>
#include <aws/core/Aws.h>  // for Aws::SDKOptions
#include "driver/framework/status.h"

namespace adbcpq {
using adbc::driver::Status;

// AWS authentication options for Redshift
struct AwsAuthOptions {
  std::string host;
  std::string port;
  std::string database;

  std::string profile;  // IAM profile name
  std::string cluster_id;
  std::string region;
  std::string user;  // UID

  // Explicit IAM
  std::string access_key_id;
  std::string secret_access_key;
};

// Redshift cluster credentials returned from GetClusterCredentials API
struct RedshiftCredentials {
  std::string db_user;
  std::string db_password;
  std::string expiration;
};

class AwsAuthClient {
 private:
  class Impl;

 public:
  static AwsAuthClient& Instance() {
    static AwsAuthClient instance;
    return instance;
  }

 private:
  AwsAuthClient();
  ~AwsAuthClient();

 public:
  // Get Redshift cluster credentials using IAM authentication by
  // calling the Redshift GetClusterCredentials API.
  Status GetRedshiftCredentials(const AwsAuthOptions& options,
                                RedshiftCredentials* out_credentials) const;

 private:
  Aws::SDKOptions options_;
  std::unique_ptr<Impl> pimpl_;
};
}  // namespace adbcpq
