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

/**
 * Structured error class for ADBC operations.
 */
export class AdbcError extends Error {
  code: string
  vendorCode?: number
  sqlState?: string

  constructor(message: string, code: string, vendorCode?: number, sqlState?: string) {
    super(message)
    this.name = 'AdbcError'
    this.code = code
    this.vendorCode = vendorCode
    this.sqlState = sqlState
  }

  /**
   * Parses a raw error message from the native binding into a structured AdbcError.
   * Expected format: "[STATUS] Message (Vendor Code: X, SQL State: Y)"
   *
   * If the error does not match this specific format (e.g. it is a standard JS TypeError),
   * the original error is returned unmodified.
   */
  static fromError(err: any): any {
    if (err instanceof Error) {
      // Regex to match: [Status] Message (Vendor Code: 123, SQL State: XYZ)
      // Note: Message might contain parentheses, so we match strictly on the suffix.
      const match = err.message.trim().match(/^\[(.*?)\] (.*) \(Vendor Code: (-?\d+), SQL State: (.*)\)$/s)

      if (match) {
        const [, status, msg, vendor, sqlState] = match
        return new AdbcError(msg, status, Number(vendor), sqlState)
      }
    }
    return err
  }
}
