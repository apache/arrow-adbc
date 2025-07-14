# Arrow Schema and Type Handling in Databricks ADBC Driver

This document outlines our findings regarding Arrow schema handling in the Databricks ADBC driver, with a particular focus on decimal type handling across different Databricks Runtime (DBR) versions.

## Background

The Databricks Thrift server uses different approaches for type handling depending on the protocol version:

- **Before Protocol V5**: No Arrow schema metadata, decimal values transmitted as strings
- **Protocol V5 and later**: Arrow schema available, decimal values can be native Arrow Decimal128 or strings

## The `arrowSchema` Field in `TGetResultSetMetadataResp`

### Purpose and Introduction

The `arrowSchema` field was added to `TGetResultSetMetadataResp` as part of the Spark protocol version 5 (SPARK_CLI_SERVICE_PROTOCOL_V5) enhancement:

```
// Spark V5:
// - Return Arrow metadata in TGetResultSetMetadataResp
// - Shortcut to return resultSetMetadata from FetchResults
// - Support for Arrow types for Timestamp, Decimal and complex types
SPARK_CLI_SERVICE_PROTOCOL_V5 = 0xA505
```

This field contains the serialized Apache Arrow schema representation of the result set, providing richer type information than the traditional `schema` field.

### Difference from Traditional Schema

1. **Different Schema Formats**:
   - `schema`: Contains the traditional Hive-style table schema using `TTableSchema`
   - `arrowSchema`: Contains the serialized Apache Arrow schema, a more modern, columnar-oriented format

2. **Type Information**:
   - `arrowSchema` contains more detailed type information, including metadata about the original SQL types
   - Arrow schema includes additional metadata like `FIELD_METADATA_SQL_NAME_KEY` and `FIELD_METADATA_SQL_JSON_TYPE`

## Decimal Type Handling

### The Issue

There is currently a bug with the ADBC driver where decimal type handling differs based on DBR version:
- For DBR v<5: Decimals are transmitted as strings
- For DBR v≥5: Decimals can be transmitted as native Decimal128 types

Currently, the driver relies on server version detection to determine how to handle decimals, but this approach is fragile and not future-proof.

### Type Representation in Arrow Schema

In the Arrow schema, decimal values can be represented in two ways:

1. **Native Decimal Representation** (when `arrowTypeOverrides.decimalAsArrow = true`):
   - Arrow Type: `Decimal128(precision=X, scale=Y)`
   - Metadata: Contains SQL type = "DECIMAL(X,Y)"

2. **String-Encoded Decimal** (when `arrowTypeOverrides.decimalAsArrow = false`):
   - Arrow Type: `Utf8` (string)
   - Metadata: Contains SQL type = "DECIMAL(X,Y)"

### How This Works in the Code

The decision to represent decimals as strings or native types is controlled by `ArrowTypeOverrides`:

```scala
val decimalAsString = !arrowTypeOverrides.decimalAsArrow.getOrElse(false)

// Determines how to represent each column type
def getOutputColumnModifier(dataType : DataType) = {
  // ...
  case _: DecimalType if decimalAsString => CastToString
  // ...
}
```

When `decimalAsString` is true, decimal fields are cast to strings before transmission.

## Solution for ADBC Driver

### Recommended Approach

Instead of relying on server version detection, the ADBC driver should:

1. **Check for Arrow Schema Presence**:
   ```java
   if (response.isSetArrowSchema()) {
     // Protocol V5 or later - use Arrow schema to determine type handling
     Schema arrowSchema = parseArrowSchema(response.getArrowSchema());
     
     // Inspect each field in the schema
     for (Field field : arrowSchema.getFields()) {
       if (field.getType() instanceof ArrowType.Decimal128) {
         // Handle as native Decimal128
       } else if (field.getType() instanceof ArrowType.Utf8) {
         // Check metadata to see if it's a string-encoded decimal
         Map<String, String> metadata = field.getMetadata();
         String sqlType = metadata.get("Spark:DataType:SqlName");
         
         if (sqlType != null && sqlType.startsWith("DECIMAL")) {
           // Handle as string-encoded decimal
         }
       }
     }
   } else {
     // Protocol before V5 - always handle decimals as strings
   }
   ```

2. **For Pre-V5 Protocol**:
   - Assume all decimal values are transmitted as strings
   - Parse decimal values from their string representations

### Benefits of This Approach

1. **More Robust**: Adapts to the actual data representation rather than making assumptions based on server versions
2. **Future-Proof**: Works correctly even if server behavior changes in future versions
3. **Accurate Type Handling**: Ensures decimal values are processed correctly regardless of how they're transmitted

## Other Types with Improved Fidelity in Arrow Schema

The Arrow schema also improves fidelity for:

1. **Timestamp Types**:
   - Traditional: Often converted to strings or long integers
   - Arrow: Native timestamp type with timezone preservation

2. **Complex Types** (Arrays, Maps, Structs):
   - Traditional: Converted to string representations
   - Arrow: Preserved as nested structures

3. **Interval Types**:
   - Traditional: String representation
   - Arrow: Native interval representation

## Parsing and Using the Arrow Schema

Parsing the Arrow schema from the binary format in `TGetResultSetMetadataResp` requires specific handling. Here's how to properly deserialize and use the Arrow schema in the ADBC driver:

### Deserializing the Arrow Schema

The `arrowSchema` field in `TGetResultSetMetadataResp` contains a serialized Arrow schema in the IPC format. To deserialize it:

```java
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;

public Schema parseArrowSchema(byte[] schemaBytes) {
  try {
    ByteArrayInputStream inputStream = new ByteArrayInputStream(schemaBytes);
    ReadChannel readChannel = new ReadChannel(Channels.newChannel(inputStream));
    return MessageSerializer.deserializeSchema(readChannel);
  } catch (IOException e) {
    throw new RuntimeException("Failed to parse Arrow schema", e);
  }
}
```

### Inspecting Field Types and Metadata

Once you have the deserialized Arrow schema, you can inspect its fields to determine the correct type handling:

```java
public boolean isDecimalField(Field field) {
  // Check if it's a native Decimal128 type
  if (field.getType() instanceof ArrowType.Decimal) {
    return true;
  }
  
  // Check if it's a string-encoded decimal
  if (field.getType() instanceof ArrowType.Utf8) {
    Map<String, String> metadata = field.getMetadata();
    if (metadata != null) {
      String sqlType = metadata.get("Spark:DataType:SqlName");
      return sqlType != null && sqlType.startsWith("DECIMAL");
    }
  }
  
  return false;
}

public DecimalInfo getDecimalInfo(Field field) {
  if (field.getType() instanceof ArrowType.Decimal) {
    ArrowType.Decimal decimalType = (ArrowType.Decimal) field.getType();
    return new DecimalInfo(
      decimalType.getPrecision(),
      decimalType.getScale(),
      false  // Not string-encoded
    );
  } else {
    // For string-encoded decimals, parse precision and scale from SQL type
    String sqlType = field.getMetadata().get("Spark:DataType:SqlName");
    // Parse "DECIMAL(p,s)" to extract precision and scale
    Pattern pattern = Pattern.compile("DECIMAL\\((\\d+),(\\d+)\\)");
    Matcher matcher = pattern.matcher(sqlType);
    if (matcher.find()) {
      int precision = Integer.parseInt(matcher.group(1));
      int scale = Integer.parseInt(matcher.group(2));
      return new DecimalInfo(precision, scale, true);  // String-encoded
    }
  }
  throw new IllegalArgumentException("Not a decimal field");
}

class DecimalInfo {
  final int precision;
  final int scale;
  final boolean isStringEncoded;
  
  DecimalInfo(int precision, int scale, boolean isStringEncoded) {
    this.precision = precision;
    this.scale = scale;
    this.isStringEncoded = isStringEncoded;
  }
}
```

### Integration with ADBC Driver

Here's how to integrate this schema parsing with the ADBC driver's type handling:

```java
public void setupTypeHandlers(TGetResultSetMetadataResp metadataResp) {
  TypeHandler[] handlers = new TypeHandler[metadataResp.getSchema().getColumnsSize()];
  
  if (metadataResp.isSetArrowSchema()) {
    // Protocol V5 or later - use Arrow schema
    Schema arrowSchema = parseArrowSchema(metadataResp.getArrowSchema());
    
    for (int i = 0; i < handlers.length; i++) {
      Field field = arrowSchema.getFields().get(i);
      handlers[i] = createTypeHandlerFromArrowField(field);
    }
  } else {
    // Protocol before V5 - use traditional schema
    for (int i = 0; i < handlers.length; i++) {
      TColumnDesc columnDesc = metadataResp.getSchema().getColumns().get(i);
      handlers[i] = createTypeHandlerFromColumnDesc(columnDesc);
    }
  }
  
  this.typeHandlers = handlers;
}

private TypeHandler createTypeHandlerFromArrowField(Field field) {
  ArrowType type = field.getType();
  
  if (type instanceof ArrowType.Decimal) {
    return new DecimalTypeHandler((ArrowType.Decimal) type);
  } else if (type instanceof ArrowType.Utf8) {
    // Check if it's a string-encoded decimal
    Map<String, String> metadata = field.getMetadata();
    if (metadata != null) {
      String sqlType = metadata.get("Spark:DataType:SqlName");
      if (sqlType != null && sqlType.startsWith("DECIMAL")) {
        return new StringDecimalTypeHandler(sqlType);
      }
    }
    return new StringTypeHandler();
  }
  // Handle other types...
}
```

### Error Handling

When parsing the Arrow schema, it's important to handle potential errors:

1. **Missing Arrow Schema**: For protocol versions before V5
2. **Deserialization Errors**: If the schema bytes are corrupted
3. **Unexpected Types**: If the schema contains types not expected by the driver

Implement proper error handling and fallback mechanisms to ensure the driver remains robust even when encountering issues with the Arrow schema.

## Conclusion

By using the Arrow schema to determine type handling, the ADBC driver can correctly process decimal values (and other types) regardless of the DBR version or protocol version. This approach is more robust and future-proof than relying on server version detection.

For DBR versions before V5 (where Arrow schema is not available), the driver should default to handling decimals as strings, as that was the only representation used in those versions.

The implementation details provided in this document should help in correctly parsing and utilizing the Arrow schema information to ensure proper type handling across all Databricks Runtime versions.