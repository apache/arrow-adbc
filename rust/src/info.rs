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

use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Int32Builder, Int64Builder, ListBuilder, MapBuilder,
    StringBuilder, UInt32BufferBuilder, UInt32Builder, UInt8BufferBuilder,
};
use arrow_array::cast::{as_primitive_array, as_string_array, as_union_array};
use arrow_array::types::UInt32Type;
use arrow_array::{Array, ArrayRef, UnionArray};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, DataType, Field, Fields, Schema, UnionFields, UnionMode};
use num_enum::{FromPrimitive, IntoPrimitive};
use once_cell::sync::Lazy;
use std::{borrow::Cow, collections::HashMap, sync::Arc};

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

static INFO_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("info_name", DataType::UInt32, false),
        Field::new(
            "info_value",
            DataType::Union(
                UnionFields::new(
                    vec![0, 1, 2, 3, 4, 5],
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
                                            DataType::List(Arc::new(Field::new(
                                                "item",
                                                DataType::Int32,
                                                true,
                                            ))),
                                            true,
                                        ),
                                    ])),
                                    false,
                                )),
                                false,
                            ),
                            true,
                        ),
                    ],
                ),
                UnionMode::Dense,
            ),
            true,
        ),
    ]))
});

/// Rust representations of database/drier metadata
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

    let mut codes = UInt32Builder::with_capacity(info_iter.size_hint().0);

    // Type id tells which array the value is in
    let mut type_id = UInt8BufferBuilder::new(info_iter.size_hint().0);
    // Value offset tells the offset of the value in the respective array
    let mut value_offsets = UInt32BufferBuilder::new(info_iter.size_hint().0);

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

    for (code, info) in info_iter {
        codes.append_value(code.into());

        match info {
            InfoData::StringValue(val) => {
                string_values.append_value(val);
                type_id.append(0);
                let value_offset = string_values.len() - 1;
                value_offsets.append(
                    value_offset
                        .try_into()
                        .expect("Array has more values than can be indexed by u32"),
                );
            }
            _ => {
                todo!("support other types in info_data")
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
    let info_schema = INFO_SCHEMA.clone();
    let union_fields = {
        match info_schema.field(1).data_type() {
            DataType::Union(fields, _) => fields,
            _ => unreachable!(),
        }
    };
    let children = union_fields
        .iter()
        .map(|f| (**f.1).clone())
        .zip(arrays.into_iter())
        .collect();

    let info_value = UnionArray::try_new(
        &[0, 1, 2, 3, 4, 5],
        type_id.finish(),
        Some(value_offsets.finish()),
        children,
    )
    .expect("Info value array is always valid.");

    let batch: RecordBatch = RecordBatch::try_new(
        info_schema,
        vec![Arc::new(codes.finish()), Arc::new(info_value)],
    )
    .expect("Info data batch is always valid.");

    let schema = batch.schema();
    RecordBatchIterator::new(std::iter::once(batch).map(Ok), schema)
}

pub fn import_info_data(
    reader: impl RecordBatchReader,
) -> Result<HashMap<InfoCode, InfoData>, ArrowError> {
    let batches = reader.collect::<Result<Vec<RecordBatch>, ArrowError>>()?;

    Ok(batches
        .iter()
        .flat_map(|batch| {
            let codes = as_primitive_array::<UInt32Type>(batch.column(0));
            let codes = codes.into_iter().map(|code| code.unwrap().into());

            let info_data = as_union_array(batch.column(1));
            let info_data = (0..info_data.len()).map(|i| -> InfoData {
                let type_id = info_data.type_id(i);
                match type_id {
                    0 => InfoData::StringValue(Cow::Owned(
                        as_string_array(&info_data.value(i)).value(0).to_string(),
                    )),
                    _ => todo!("Support other types"),
                }
            });

            std::iter::zip(codes, info_data)
        })
        .collect())
}

#[cfg(test)]
mod test {
    use arrow_array::cast::{as_primitive_array, as_string_array, as_union_array};
    use arrow_array::types::UInt32Type;

    use super::*;

    #[test]
    fn test_export_info_data() {
        let example_info = vec![
            (
                InfoCode::VendorName,
                InfoData::StringValue(Cow::Borrowed("test vendor")),
            ),
            (
                InfoCode::DriverName,
                InfoData::StringValue(Cow::Borrowed("test driver")),
            ),
        ];

        let info = export_info_data(example_info.clone());

        assert_eq!(info.schema(), *INFO_SCHEMA);
        let info: HashMap<InfoCode, String> = info
            .flat_map(|maybe_batch| {
                let batch = maybe_batch.unwrap();
                let id = as_primitive_array::<UInt32Type>(batch.column(0));
                let values = as_union_array(batch.column(1));
                let string_values = as_string_array(values.child(0));
                let mut out = vec![];
                for i in 0..batch.num_rows() {
                    assert_eq!(values.type_id(i), 0);
                    let code = InfoCode::from(id.value(i));
                    out.push((code, string_values.value(i).to_string()));
                }
                out
            })
            .collect();

        assert_eq!(
            info.get(&InfoCode::VendorName),
            Some(&"test vendor".to_string())
        );
        assert_eq!(
            info.get(&InfoCode::DriverName),
            Some(&"test driver".to_string())
        );

        let info = export_info_data(example_info);

        let info: HashMap<InfoCode, InfoData> =
            import_info_data(info).unwrap().into_iter().collect();

        assert_eq!(
            info.get(&InfoCode::VendorName),
            Some(&InfoData::StringValue(Cow::Owned(
                "test vendor".to_string()
            )))
        );
        assert_eq!(
            info.get(&InfoCode::DriverName),
            Some(&InfoData::StringValue(Cow::Owned(
                "test driver".to_string()
            )))
        );
    }
}
