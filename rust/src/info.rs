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

//! Utilities for driver info
//!
//! For use with [crate::AdbcConnection::get_info].

use std::{borrow::Cow, collections::HashMap, sync::Arc};

use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Int32BufferBuilder, Int32Builder, Int64Builder,
    Int8BufferBuilder, ListBuilder, MapBuilder, StringBuilder, UInt32Builder,
};
use arrow_array::cast::{
    as_boolean_array, as_list_array, as_map_array, as_primitive_array, as_string_array,
    as_union_array,
};
use arrow_array::types::{Int32Type, Int64Type, UInt32Type};
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchReader, UnionArray};
use arrow_schema::{ArrowError, DataType, Field, Fields, Schema, UnionFields, UnionMode};
use num_enum::{FromPrimitive, IntoPrimitive};
use once_cell::sync::Lazy;

use crate::utils::SingleBatchReader;

/// Contains known info codes defined by ADBC.
#[repr(u32)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, FromPrimitive, IntoPrimitive, Hash)]
pub enum InfoCode {
    /// The database vendor/product version (type: utf8).
    VendorName = 0,
    /// The database vendor/product version (type: utf8).
    VendorVersion = 1,
    /// The database vendor/product Arrow library version (type: utf8).
    VendorArrowVersion = 2,
    /// The driver name (type: utf8).
    DriverName = 100,
    /// The driver version (type: utf8).
    DriverVersion = 101,
    /// The driver Arrow library version (type: utf8).
    DriverArrowVersion = 102,
    /// Some other info code.
    #[num_enum(catch_all)]
    Other(u32),
}

static UNION_FIELDS: Lazy<Vec<Field>> = Lazy::new(|| {
    vec![
        Field::new("string_value", DataType::Utf8, true),
        Field::new("bool_value", DataType::Boolean, true),
        Field::new("int64_value", DataType::Int64, true),
        Field::new("int32_bitmask", DataType::Int32, true),
        Field::new(
            "string_list",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "int32_to_int32_list_map",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("keys", DataType::Int32, false),
                        Field::new(
                            "values",
                            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                            true,
                        ),
                    ])),
                    false,
                )),
                false,
            ),
            true,
        ),
    ]
});

static INFO_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("info_name", DataType::UInt32, false),
        Field::new(
            "info_value",
            DataType::Union(
                UnionFields::new(vec![0, 1, 2, 3, 4, 5], UNION_FIELDS.clone()),
                UnionMode::Dense,
            ),
            true,
        ),
    ]))
});

/// Rust representations of database/driver metadata
#[derive(Clone, Debug, PartialEq)]
pub enum InfoData {
    StringValue(Cow<'static, str>),
    BoolValue(bool),
    Int64Value(i64),
    Int32Bitmask(i32),
    StringList(Vec<String>),
    Int32ToInt32ListMap(HashMap<i32, Vec<i32>>),
}

pub fn export_info_data(
    info_iter: impl IntoIterator<Item = (InfoCode, InfoData)>,
) -> impl RecordBatchReader {
    let info_iter = info_iter.into_iter();
    let (min_len, _) = info_iter.size_hint();

    let mut codes = UInt32Builder::with_capacity(min_len);

    // Type id tells which array the value is in
    let mut type_id = Int8BufferBuilder::new(min_len);
    // Value offset tells the offset of the value in the respective array
    let mut value_offsets = Int32BufferBuilder::new(min_len);

    // Make one builder per child of union array. Will combine after.
    let mut string_values = StringBuilder::new();
    let mut bool_values = BooleanBuilder::new();
    let mut int64_values = Int64Builder::new();
    let mut int32_bitmasks = Int32Builder::new();
    let mut string_lists = ListBuilder::new(StringBuilder::new());
    let mut int32_to_int32_list_maps = MapBuilder::new(
        None,
        Int32Builder::new(),
        ListBuilder::new(Int32Builder::new()),
    );

    let overflow_msg = "Array has more values than can be indexed by i32";

    for (code, info) in info_iter {
        codes.append_value(code.into());

        match info {
            InfoData::StringValue(val) => {
                string_values.append_value(val);
                type_id.append(0);
                let value_offset = string_values.len() - 1;
                value_offsets.append(value_offset.try_into().expect(overflow_msg));
            }
            InfoData::BoolValue(val) => {
                bool_values.append_value(val);
                type_id.append(1);
                let value_offset = bool_values.len() - 1;
                value_offsets.append(value_offset.try_into().expect(overflow_msg));
            }
            InfoData::Int64Value(val) => {
                int64_values.append_value(val);
                type_id.append(2);
                let value_offset = int64_values.len() - 1;
                value_offsets.append(value_offset.try_into().expect(overflow_msg));
            }
            InfoData::Int32Bitmask(val) => {
                int32_bitmasks.append_value(val);
                type_id.append(3);
                let value_offset = int32_bitmasks.len() - 1;
                value_offsets.append(value_offset.try_into().expect(overflow_msg));
            }
            InfoData::StringList(val) => {
                string_lists.append_value(val.iter().map(Some));
                type_id.append(4);
                let value_offset = string_lists.len() - 1;
                value_offsets.append(value_offset.try_into().expect(overflow_msg));
            }
            InfoData::Int32ToInt32ListMap(val) => {
                for (k, v) in val {
                    int32_to_int32_list_maps.keys().append_value(k);
                    int32_to_int32_list_maps
                        .values()
                        .append_value(v.into_iter().map(Some));
                }
                int32_to_int32_list_maps
                    .append(true)
                    .expect("Map builder is in an inconsistent state");
                type_id.append(5);
                let value_offset = int32_to_int32_list_maps.len() - 1;
                value_offsets.append(value_offset.try_into().expect(overflow_msg));
            }
        };
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(string_values.finish()),
        Arc::new(bool_values.finish()),
        Arc::new(int64_values.finish()),
        Arc::new(int32_bitmasks.finish()),
        Arc::new(string_lists.finish()),
        Arc::new(int32_to_int32_list_maps.finish()),
    ];
    let childrens = UNION_FIELDS.clone().into_iter().zip(arrays).collect();

    let array = UnionArray::try_new(
        &[0, 1, 2, 3, 4, 5],
        type_id.finish(),
        Some(value_offsets.finish()),
        childrens,
    )
    .expect("Info value array is always valid.");

    let batch: RecordBatch = RecordBatch::try_new(
        INFO_SCHEMA.clone(),
        vec![Arc::new(codes.finish()), Arc::new(array)],
    )
    .expect("Info data batch is always valid.");

    SingleBatchReader::new(batch)
}

pub fn import_info_data(
    reader: impl RecordBatchReader,
) -> Result<HashMap<InfoCode, InfoData>, ArrowError> {
    let batches = reader.collect::<Result<Vec<RecordBatch>, ArrowError>>()?;

    Ok(batches
        .iter()
        .flat_map(|batch| {
            let codes = as_primitive_array::<UInt32Type>(batch.column(0));
            let codes = codes
                .into_iter()
                .map(|code| code.expect("InfoCode must not be null").into());

            let info_data = as_union_array(batch.column(1));
            let info_data = (0..info_data.len()).map(|i| -> InfoData {
                let type_id = info_data.type_id(i);
                match type_id {
                    0 => InfoData::StringValue(Cow::Owned(
                        as_string_array(&info_data.value(i)).value(0).to_string(),
                    )),
                    1 => InfoData::BoolValue(as_boolean_array(&info_data.value(i)).value(0)),
                    2 => InfoData::Int64Value(
                        as_primitive_array::<Int64Type>(&info_data.value(i)).value(0),
                    ),
                    3 => InfoData::Int32Bitmask(
                        as_primitive_array::<Int32Type>(&info_data.value(i)).value(0),
                    ),
                    4 => {
                        let elem = as_list_array(&info_data.value(i)).value(0);
                        InfoData::StringList(
                            as_string_array(&elem)
                                .iter()
                                .map(|e| e.expect("String must not be missing").to_string())
                                .collect(),
                        )
                    }
                    5 => {
                        let struct_array = as_map_array(&info_data.value(i)).value(0);
                        let keys = as_primitive_array::<Int32Type>(struct_array.column(0))
                            .iter()
                            .map(|k| k.expect("Key must not be missing"));
                        let values = as_list_array(struct_array.column(1)).iter().map(|v| {
                            as_primitive_array::<Int32Type>(&v.expect("Value must not be missing"))
                                .iter()
                                .map(|v| v.expect("List element must not be missing"))
                                .collect()
                        });
                        InfoData::Int32ToInt32ListMap(keys.zip(values).collect())
                    }
                    _ => unreachable!("Incorrect type id"),
                }
            });

            std::iter::zip(codes, info_data)
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_export_import_roundtrip() {
        let tests = [
            vec![
                (
                    InfoCode::VendorName,
                    InfoData::StringValue("vendor test".into()),
                ),
                (InfoCode::VendorArrowVersion, InfoData::BoolValue(true)),
                (InfoCode::DriverArrowVersion, InfoData::Int64Value(42)),
                (InfoCode::VendorVersion, InfoData::Int32Bitmask(1337)),
                (
                    InfoCode::DriverVersion,
                    InfoData::StringList(vec!["hello".into(), "world".into()]),
                ),
                (
                    InfoCode::DriverName,
                    InfoData::Int32ToInt32ListMap(
                        [(0, vec![1, 2, 3]), (42, vec![])].into_iter().collect(),
                    ),
                ),
            ],
            vec![
                (InfoCode::VendorName, InfoData::StringList(vec![])),
                (
                    InfoCode::VendorArrowVersion,
                    InfoData::Int32ToInt32ListMap(HashMap::new()),
                ),
            ],
            vec![],
        ];

        for test in tests {
            let test: HashMap<InfoCode, InfoData> = test.into_iter().collect();

            let batch = export_info_data(test.clone());
            assert_eq!(batch.schema(), *INFO_SCHEMA);

            let map = import_info_data(batch).unwrap();
            assert_eq!(test, map);
        }
    }
}
