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
   * Promotes a raw error thrown by the native binding into a structured AdbcError.
   *
   * Non-ADBC errors (e.g. a standard JS TypeError) are returned unmodified.
   */
  static fromError(err: any): any {
    if (err instanceof Error && err.name === 'AdbcError') {
      const e = err as any
      return new AdbcError(e.message, e.code ?? 'UNKNOWN', e.vendorCode, e.sqlState)
    }
    return err
  }
}
