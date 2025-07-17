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

#ifdef ADBC_REDSHIFT_FLAVOR
#include "redshift_auth.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/redshift/RedshiftClient.h>
#include <aws/redshift/model/GetClusterCredentialsRequest.h>

#include <memory>
#include <string>

namespace adbcpq {

class AwsAuthClient::Impl {
 public:
  Status GetRedshiftCredentials(const AwsAuthOptions& aws_options,
                                RedshiftCredentials* out_credentials) const {
    try {
      if (!aws_options.profile.empty()) {
        Aws::Auth::AWSCredentials aws_credentials;
        auto status = GetCredentialsFromProfile(aws_options.profile, &aws_credentials);
        if (!status.ok()) {
          return status;
        }
        return GetClusterCredentials(aws_options, aws_credentials, out_credentials);
      }

      if (aws_options.access_key_id.empty() || aws_options.secret_access_key.empty()) {
        return Status::InvalidArgument(
            "[aws] Both access_key_id and secret_access_key must be provided when not "
            "using AWS profile");
      }
      Aws::Auth::AWSCredentials aws_credentials(aws_options.access_key_id,
                                                aws_options.secret_access_key);
      return GetClusterCredentials(aws_options, aws_credentials, out_credentials);
    } catch (const std::exception& e) {
      return Status::IO("Redshift IAM authentication: ", e.what());
    }
  }

 private:
  Status GetCredentialsFromProfile(const std::string& profile_name,
                                   Aws::Auth::AWSCredentials* out_credentials) const {
    // Create credentials provider for the specified profile
    auto credentials_provider =
        Aws::MakeShared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>(
            "RedshiftIAMAuth", profile_name.c_str());

    *out_credentials = credentials_provider->GetAWSCredentials();
    if (out_credentials->GetAWSAccessKeyId().empty() ||
        out_credentials->GetAWSSecretKey().empty()) {
      return Status::IO("[aws] Failed to get AWS credentials from profile: ",
                        profile_name);
    }
    return Status::Ok();
  }

  Status GetClusterCredentials(const AwsAuthOptions& aws_options,
                               const Aws::Auth::AWSCredentials& aws_credentials,
                               RedshiftCredentials* out_credentials) const {
    Aws::Client::ClientConfiguration config;
    if (!aws_options.region.empty()) {
      config.region = aws_options.region;
    }

    Aws::Redshift::RedshiftClient redshift_client(aws_credentials, config);
    Aws::Redshift::Model::GetClusterCredentialsRequest request;

    // Strip 'IAM:' prefix if provided
    std::string db_user = aws_options.user.empty() ? "awsuser" : aws_options.user;
    if (db_user.size() >= 4 && db_user.substr(0, 4) == "IAM:") {
      db_user = db_user.substr(4);
    }

    request.SetClusterIdentifier(aws_options.cluster_id);
    request.SetDbUser(db_user);
    request.SetDbName(aws_options.database);

    auto outcome = redshift_client.GetClusterCredentials(request);
    if (!outcome.IsSuccess()) {
      return Status::IO("[aws] Failed to get cluster credentials: ",
                        outcome.GetError().GetMessage());
    }

    // Extract the credentials
    auto& result = outcome.GetResult();
    out_credentials->db_user = result.GetDbUser();
    out_credentials->db_password = result.GetDbPassword();
    if (result.GetExpiration().WasParseSuccessful()) {
      out_credentials->expiration =
          result.GetExpiration().ToGmtString(Aws::Utils::DateFormat::ISO_8601);
    }

    return Status::Ok();
  }
};

AwsAuthClient::AwsAuthClient() : options_{}, pimpl_(nullptr) {
  // ..set options here...
  Aws::InitAPI(options_);
  pimpl_ = std::make_unique<Impl>();
}

AwsAuthClient::~AwsAuthClient() { Aws::ShutdownAPI(options_); }

Status AwsAuthClient::GetRedshiftCredentials(const AwsAuthOptions& settings,
                                             RedshiftCredentials* credentials) const {
  return pimpl_->GetRedshiftCredentials(settings, credentials);
}
}  // namespace adbcpq
#endif  // ADBC_REDSHIFT_FLAVOR
