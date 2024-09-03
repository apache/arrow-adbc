<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# ADBC Snowflake Driver

This driver provides an interface to
[BigQuery](https://cloud.google.com/bigquery) using ADBC.

## Building

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for details.

## Testing

BigQuery credentials and project ID are required.

### Environment Variables
#### Project ID
Set `BIGQUERY_PROJECT_ID` to the project ID.

#### Authentication
Set either one following environment variables for authentication:

##### BIGQUERY_JSON_CREDENTIAL_FILE
Path to the JSON credential file. This file can be generated using `gcloud`:

```sh
gcloud auth application-default login
```

And the default location of the generated JSON credential file is located at

```sh
$HOME/.config/gcloud/application_default_credentials.json
```

##### BIGQUERY_JSON_CREDENTIAL_STRING
Store the whole JSON credential content, something like

```json
{
  "account": "",
  "client_id": "123456789012-1234567890abcdefabcdefabcdefabcd.apps.googleusercontent.com",
  "client_secret": "d-SECRETSECRETSECRETSECR",
  "refresh_token": "1//1234567890abcdefabcdefabcdef-abcdefabcd-abcdefabcdefabcdefabcdefab-abcdefabcdefabcdefabcdefabcdef-ab",
  "type": "authorized_user",
  "universe_domain": "googleapis.com"
}
```
