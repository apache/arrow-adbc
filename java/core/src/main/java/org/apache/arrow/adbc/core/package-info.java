/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * ADBC (Arrow Database Connectivity) is an API standard for Arrow-based database access.
 *
 * <p>An Arrow-based interface between applications and database drivers. ADBC aims to provide a
 * vendor-independent API for SQL and Substrait-based database access that is targeted at
 * analytics/OLAP use cases.
 *
 * <p>This API is intended to be implemented directly by drivers and used directly by client
 * applications. To assist portability between different vendors, a "driver manager" library is also
 * provided, which implements this same API, but dynamically loads drivers internally and forwards
 * calls appropriately.
 *
 * @version 1.0.0
 */
package org.apache.arrow.adbc.core;
