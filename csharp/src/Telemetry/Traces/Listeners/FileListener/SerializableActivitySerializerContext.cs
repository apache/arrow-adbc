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

using System;
using System.Text.Json.Serialization;

namespace Apache.Arrow.Adbc.Telemetry.Traces.Listeners.FileListener
{
    [JsonSerializable(typeof(SerializableActivity))]

    [JsonSerializable(typeof(string))]
    [JsonSerializable(typeof(byte))]
    [JsonSerializable(typeof(sbyte))]
    [JsonSerializable(typeof(ushort))]
    [JsonSerializable(typeof(short))]
    [JsonSerializable(typeof(uint))]
    [JsonSerializable(typeof(int))]
    [JsonSerializable(typeof(ulong))]
    [JsonSerializable(typeof(long))]
    [JsonSerializable(typeof(ulong))]
    [JsonSerializable(typeof(float))]
    [JsonSerializable(typeof(double))]
    [JsonSerializable(typeof(decimal))]
    [JsonSerializable(typeof(char))]
    [JsonSerializable(typeof(bool))]

    [JsonSerializable(typeof(string[]))]
    [JsonSerializable(typeof(byte[]))]
    [JsonSerializable(typeof(sbyte[]))]
    [JsonSerializable(typeof(ushort[]))]
    [JsonSerializable(typeof(short[]))]
    [JsonSerializable(typeof(uint[]))]
    [JsonSerializable(typeof(int[]))]
    [JsonSerializable(typeof(ulong[]))]
    [JsonSerializable(typeof(long[]))]
    [JsonSerializable(typeof(ulong[]))]
    [JsonSerializable(typeof(float[]))]
    [JsonSerializable(typeof(double[]))]
    [JsonSerializable(typeof(decimal[]))]
    [JsonSerializable(typeof(char[]))]
    [JsonSerializable(typeof(bool[]))]

    [JsonSerializable(typeof(Uri))]
    internal partial class SerializableActivitySerializerContext : JsonSerializerContext
    {
    }
}
