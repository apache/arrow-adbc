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
package org.apache.arrow.adbc.driver.flightsql;

import com.github.benmanes.caffeine.cache.LoadingCache;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.StandardSchemas;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

final class GetObjectsMetadataReaders {

  static ArrowReader CreateGetObjectsReader(
      BufferAllocator allocator,
      FlightSqlClientWithCallOptions client,
      LoadingCache<Location, FlightSqlClientWithCallOptions> clientCache,
      final AdbcConnection.GetObjectsDepth depth,
      final String catalogPattern,
      final String dbSchemaPattern,
      final String tableNamePattern,
      final String[] tableTypes,
      final String columnNamePattern)
      throws AdbcException {
    switch (depth) {
      case CATALOGS:
        return new GetCatalogsMetadataReader(allocator, client, clientCache, catalogPattern);
      case DB_SCHEMAS:
        return new GetDbSchemasMetadataReader(
            allocator, client, clientCache, catalogPattern, dbSchemaPattern);
      case TABLES:
        return new GetTablesMetadataReader(
            allocator,
            client,
            clientCache,
            catalogPattern,
            dbSchemaPattern,
            tableNamePattern,
            tableTypes);
      case ALL:
        return new GetTablesMetadataReader(
            allocator,
            client,
            clientCache,
            catalogPattern,
            dbSchemaPattern,
            tableNamePattern,
            tableTypes,
            columnNamePattern);
      default:
        return null;
    }
  }

  private abstract static class GetObjectMetadataReader extends BaseFlightReader {
    private final VectorSchemaRoot aggregateRoot;
    protected final Text buffer = new Text();

    protected GetObjectMetadataReader(
        BufferAllocator allocator,
        FlightSqlClientWithCallOptions client,
        LoadingCache<Location, FlightSqlClientWithCallOptions> clientCache,
        Supplier<List<FlightEndpoint>> rpcCall)
        throws AdbcException {
      super(allocator, client, clientCache, rpcCall);
      aggregateRoot = VectorSchemaRoot.create(readSchema(), allocator);
      populateEndpointData();
    }

    @Override
    public void close() throws IOException {
      Exception caughtException = null;
      try {
        AutoCloseables.close(aggregateRoot);
      } catch (Exception ex) {
        caughtException = ex;
      }
      super.close();
      if (caughtException != null) {
        throw new RuntimeException(caughtException);
      }
    }

    @Override
    public boolean loadNextBatch() throws IOException {
      while (super.loadNextBatch()) {
        // Do nothing. Just iterate through all partitions, processing the data.
      }
      try {
        finish();
      } catch (AdbcException e) {
        throw new RuntimeException(e);
      }
      loadRoot(aggregateRoot);
      return true;
    }

    @Override
    protected Schema readSchema() {
      return StandardSchemas.GET_OBJECTS_SCHEMA;
    }

    protected void finish() throws AdbcException, IOException {}

    protected VectorSchemaRoot getAggregateRoot() {
      return aggregateRoot;
    }

    protected static Pattern toPattern(String filterPattern) {
      return filterPattern != null
          ? Pattern.compile(Pattern.quote(filterPattern).replace("_", ".").replace("%", ".*"))
          : null;
    }
  }

  private static class GetCatalogsMetadataReader extends GetObjectMetadataReader {
    private final Pattern catalogPattern;

    protected GetCatalogsMetadataReader(
        BufferAllocator allocator,
        FlightSqlClientWithCallOptions client,
        LoadingCache<Location, FlightSqlClientWithCallOptions> clientCache,
        String catalog)
        throws AdbcException {
      super(allocator, client, clientCache, () -> doRequest(client));
      catalogPattern = toPattern(catalog);
    }

    @Override
    protected void processRootFromStream(VectorSchemaRoot root) {
      VarCharVector catalogVector = (VarCharVector) root.getVector(0);
      VarCharVector adbcCatalogNames = (VarCharVector) getAggregateRoot().getVector(0);
      for (int srcIndex = 0, dstIndex = 0; srcIndex < root.getRowCount(); ++srcIndex) {
        catalogVector.read(srcIndex, buffer);
        if (catalogPattern.matcher(buffer.toString()).matches()) {
          catalogVector.makeTransferPair(adbcCatalogNames).copyValueSafe(srcIndex, dstIndex++);
        }
      }
    }

    private static List<FlightEndpoint> doRequest(FlightSqlClientWithCallOptions client) {
      return client.getCatalogs().getEndpoints();
    }
  }

  private static class GetDbSchemasMetadataReader extends GetObjectMetadataReader {
    private final String catalog;
    private Map<String, List<String>> catalogToSchemaMap = new LinkedHashMap<>();

    protected GetDbSchemasMetadataReader(
        BufferAllocator allocator,
        FlightSqlClientWithCallOptions client,
        LoadingCache<Location, FlightSqlClientWithCallOptions> clientCache,
        String catalog,
        String schemaPattern)
        throws AdbcException {
      super(allocator, client, clientCache, () -> doRequest(client, catalog, schemaPattern));
      this.catalog = catalog;
    }

    @Override
    protected void processRootFromStream(VectorSchemaRoot root) {
      VarCharVector catalogVector = (VarCharVector) root.getVector(0);
      VarCharVector schemaVector = (VarCharVector) root.getVector(1);
      for (int i = 0; i < root.getRowCount(); ++i) {
        String catalog;
        if (catalogVector.isNull(i)) {
          catalog = "";
        } else {
          catalogVector.read(i, buffer);
          catalog = buffer.toString();
        }
        schemaVector.read(i, buffer);
        String schema = buffer.toString();
        catalogToSchemaMap.compute(
            catalog,
            (k, v) -> {
              if (v == null) {
                return Arrays.asList(schema);
              }
              v.add(schema);
              return v;
            });
      }
    }

    @Override
    protected void finish() throws AdbcException, IOException {
      // Create a catalog-only reader to get the list of catalogs, including empty ones.
      // Then transfer the contents of this to the current reader's root.
      VarCharVector outputCatalogColumn = (VarCharVector) getAggregateRoot().getVector(0);
      try (GetCatalogsMetadataReader catalogReader =
          new GetCatalogsMetadataReader(allocator, client, clientCache, catalog)) {
        getAggregateRoot().setRowCount(catalogReader.getAggregateRoot().getRowCount());
        VarCharVector catalogColumn = (VarCharVector) catalogReader.getAggregateRoot().getVector(0);
        catalogColumn.makeTransferPair(outputCatalogColumn).transfer();
      }

      // Track visited catalogs to see if there were any encountered during processRoots that didn't
      // get reported by the GetCatalogs() RPC call.
      Set<String> visitedCatalogs = new LinkedHashSet<>();

      // Now map catalog names to schema lists.
      UnionListWriter adbcCatalogDbSchemasWriter =
          ((ListVector) getAggregateRoot().getVector(1)).getWriter();
      BaseWriter.StructWriter adbcCatalogDbSchemasStructWriter =
          adbcCatalogDbSchemasWriter.struct();
      VarCharWriter adbcCatalogDbSchemaNameWriter =
          adbcCatalogDbSchemasStructWriter.varChar("db_schema_name");
      for (int i = 0; i < getAggregateRoot().getRowCount(); ++i) {
        outputCatalogColumn.read(i, buffer);
        String catalog = buffer.toString();
        List<String> schemas = catalogToSchemaMap.get(catalog);
        if (schemas != null) {
          adbcCatalogDbSchemasWriter.startList();
          for (String schema : schemas) {
            adbcCatalogDbSchemasStructWriter.start();
            adbcCatalogDbSchemaNameWriter.writeVarChar(schema);
            adbcCatalogDbSchemasStructWriter.end();
          }
          adbcCatalogDbSchemasWriter.endList();
        }
        visitedCatalogs.add(catalog);
      }

      // If there were any catalogs that had schemas, but didn't show up in getCatalogs(), add them
      // to the vector.
      catalogToSchemaMap.keySet().removeAll(visitedCatalogs);
      int i = getAggregateRoot().getRowCount();
      getAggregateRoot().setRowCount(i + catalogToSchemaMap.size());
      for (Map.Entry<String, List<String>> entry : catalogToSchemaMap.entrySet()) {
        outputCatalogColumn.setSafe(i, entry.getKey().getBytes(StandardCharsets.UTF_8));
        adbcCatalogDbSchemasWriter.startList();
        for (String schema : entry.getValue()) {
          adbcCatalogDbSchemasStructWriter.start();
          adbcCatalogDbSchemaNameWriter.writeVarChar(schema);
          adbcCatalogDbSchemasStructWriter.end();
        }
        adbcCatalogDbSchemasWriter.endList();
      }
    }

    private static List<FlightEndpoint> doRequest(
        FlightSqlClientWithCallOptions client, String catalog, String schemaPattern) {
      return client.getSchemas(catalog, schemaPattern).getEndpoints();
    }
  }

  private static class GetTablesMetadataReader extends GetObjectMetadataReader {
    private final Pattern columnNamePattern;
    private final boolean shouldGetColumns;

    protected GetTablesMetadataReader(
        BufferAllocator allocator,
        FlightSqlClientWithCallOptions client,
        LoadingCache<Location, FlightSqlClientWithCallOptions> clientCache,
        String catalogPattern,
        String schemaPattern,
        String tablePattern,
        String[] tableTypes)
        throws AdbcException {
      super(
          allocator,
          client,
          clientCache,
          () -> doRequest(client, catalogPattern, schemaPattern, tablePattern, tableTypes, false));
      columnNamePattern = null;
      shouldGetColumns = false;
    }

    protected GetTablesMetadataReader(
        BufferAllocator allocator,
        FlightSqlClientWithCallOptions client,
        LoadingCache<Location, FlightSqlClientWithCallOptions> clientCache,
        String catalogPattern,
        String schemaPattern,
        String tablePattern,
        String[] tableTypes,
        String columnPattern)
        throws AdbcException {
      super(
          allocator,
          client,
          clientCache,
          () -> doRequest(client, catalogPattern, schemaPattern, tablePattern, tableTypes, true));
      columnNamePattern = toPattern(columnPattern);
      shouldGetColumns = true;
    }

    @Override
    protected void processRootFromStream(VectorSchemaRoot root) {}

    private static List<FlightEndpoint> doRequest(
        FlightSqlClientWithCallOptions client,
        String catalog,
        String schemaPattern,
        String table,
        String[] tableTypes,
        boolean shouldGetColumns) {
      return client
          .getTables(catalog, schemaPattern, table, Arrays.asList(tableTypes), shouldGetColumns)
          .getEndpoints();
    }
  }

  //  private final FlightSqlClientWithCallOptions client;
  //  private final VectorSchemaRoot root;
  //  private final VarCharVector adbcCatalogNames;
  //  private final UnionListWriter adbcCatalogDbSchemasWriter;
  //  private final BaseWriter.StructWriter adbcCatalogDbSchemasStructWriter;
  //  private final BaseWriter.ListWriter adbcCatalogDbSchemaTablesWriter;
  //  private final VarCharWriter adbcCatalogDbSchemaNameWriter;
  //  private final BaseWriter.StructWriter adbcTablesStructWriter;
  //  private final VarCharWriter adbcTableNameWriter;
  //  private final VarCharWriter adbcTableTypeWriter;
  //  private final BaseWriter.ListWriter adbcTableColumnsWriter;
  //  private final BufferAllocator allocator;
  //  private final AdbcConnection.GetObjectsDepth depth;
  //  private final Pattern precompiledColumnNamePattern;
  //
  //  GetObjectsMetadataReaders(
  //      BufferAllocator allocator,
  //      FlightSqlClientWithCallOptions client,
  //      LoadingCache<Location, FlightSqlClientWithCallOptions> clientCache,
  //      final AdbcConnection.GetObjectsDepth depth,
  //      final String catalogPattern,
  //      final String dbSchemaPattern,
  //      final String tableNamePattern,
  //      final String[] tableTypes,
  //      final String columnNamePattern) {
  //    super(allocator, client, clientCache, () -> doRequest(client, depth, catalogPattern,
  // dbSchemaPattern, tableNamePattern, tableTypes));
  //    this.allocator = allocator;
  //    this.client = client;
  //    this.depth = depth;
  //    this.precompiledColumnNamePattern =
  //        columnNamePattern != null && this.depth == AdbcConnection.GetObjectsDepth.ALL
  //            ? Pattern.compile(Pattern.quote(columnNamePattern).replace("_", ".").replace("%",
  // ".*"))
  //            : null;
  //    this.root = VectorSchemaRoot.create(StandardSchemas.GET_OBJECTS_SCHEMA, allocator);
  //    this.adbcCatalogNames = (VarCharVector) root.getVector(0);
  //    this.adbcCatalogDbSchemasWriter = ((ListVector) root.getVector(1)).getWriter();
  //    this.adbcCatalogDbSchemasStructWriter = adbcCatalogDbSchemasWriter.struct();
  //    this.adbcCatalogDbSchemaTablesWriter =
  //        adbcCatalogDbSchemasStructWriter.list("db_schema_tables");
  //    this.adbcCatalogDbSchemaNameWriter =
  // adbcCatalogDbSchemasStructWriter.varChar("db_schema_name");
  //    this.adbcTablesStructWriter = adbcCatalogDbSchemaTablesWriter.struct();
  //    this.adbcTableNameWriter = adbcTablesStructWriter.varChar("table_name");
  //    this.adbcTableTypeWriter = adbcTablesStructWriter.varChar("table_type");
  //    this.adbcTableColumnsWriter = adbcTablesStructWriter.list("table_columns");
  //  }
  //
  //  private void writeVarChar(VarCharWriter writer, NullableVarCharHolder value) {
  //    writer.writeVarChar(0, value.length, tempBuf);
  //  }
  //
  //  @Override
  //  protected void processRootFromStream(VectorSchemaRoot root) {
  //
  //  }
  //
  //  /** The caller must close the returned root. */
  //  VectorSchemaRoot build() throws AdbcException {
  //    // TODO Catalogs and schemas that don't contain tables are being left out
  //    byte[] lastCatalogAdded = null;
  //    byte[] lastDbSchemaAdded = null;
  //    int catalogIndex = 0;
  //
  //    for (FlightEndpoint endpoint : info.getEndpoints()) {
  //      try (FlightStream stream = client.getStream(endpoint.getTicket())) {
  //        while (stream.next()) {
  //          try (VectorSchemaRoot res = stream.getRoot()) {
  //            VarCharVector catalogVector = (VarCharVector) res.getVector(0);
  //
  //            for (int i = 0; i < res.getRowCount(); i++) {
  //              byte[] catalog = catalogVector.get(i);
  //
  //              if (i == 0 || lastCatalogAdded != catalog) {
  //                if (catalog == null) {
  //                  adbcCatalogNames.setNull(catalogIndex);
  //                } else {
  //                  adbcCatalogNames.setSafe(catalogIndex, catalog);
  //                }
  //                if (depth == AdbcConnection.GetObjectsDepth.CATALOGS) {
  //                  adbcCatalogDbSchemasWriter.writeNull();
  //                } else {
  //                  if (catalogIndex != 0) {
  //                    adbcCatalogDbSchemasWriter.endList();
  //                  }
  //                  adbcCatalogDbSchemasWriter.startList();
  //                  lastDbSchemaAdded = null;
  //                }
  //                catalogIndex++;
  //                lastCatalogAdded = catalog;
  //              }
  //
  //              if (depth != AdbcConnection.GetObjectsDepth.CATALOGS) {
  //                VarCharVector dbSchemaVector = (VarCharVector) res.getVector(1);
  //                byte[] dbSchema = dbSchemaVector.get(i);
  //
  //                if (!Arrays.equals(lastDbSchemaAdded, dbSchema)) {
  //                  if (i != 0) {
  //                    adbcCatalogDbSchemaTablesWriter.endList();
  //                    adbcCatalogDbSchemasStructWriter.end();
  //                  }
  //                  adbcCatalogDbSchemasStructWriter.start();
  //                  writeVarChar(
  //                      adbcCatalogDbSchemaNameWriter, new String(dbSchema,
  // StandardCharsets.UTF_8));
  //                  if (depth == AdbcConnection.GetObjectsDepth.DB_SCHEMAS) {
  //                    adbcCatalogDbSchemaTablesWriter.writeNull();
  //                  } else {
  //                    adbcCatalogDbSchemaTablesWriter.startList();
  //                  }
  //
  //                  lastDbSchemaAdded = dbSchema;
  //                }
  //              }
  //
  //              if (depth != AdbcConnection.GetObjectsDepth.CATALOGS
  //                  && depth != AdbcConnection.GetObjectsDepth.DB_SCHEMAS) {
  //                VarCharVector tableNameVector = (VarCharVector) res.getVector(2);
  //                VarCharVector tableTypeVector = (VarCharVector) res.getVector(3);
  //
  //                adbcTablesStructWriter.start();
  //                writeVarChar(
  //                    adbcTableNameWriter,
  //                    new String(tableNameVector.get(i), StandardCharsets.UTF_8));
  //                writeVarChar(
  //                    adbcTableTypeWriter,
  //                    new String(tableTypeVector.get(i), StandardCharsets.UTF_8));
  //
  //                if (depth == AdbcConnection.GetObjectsDepth.ALL) {
  //                  VarBinaryVector tableSchemaVector = (VarBinaryVector) res.getVector(4);
  //                  Schema schema;
  //
  //                  try {
  //                    schema =
  //                        MessageSerializer.deserializeSchema(
  //                            new ReadChannel(
  //                                Channels.newChannel(
  //                                    new ByteArrayInputStream(tableSchemaVector.get(i)))));
  //                  } catch (IOException e) {
  //                    throw new RuntimeException(e);
  //                  }
  //
  //                  adbcTableColumnsWriter.startList();
  //
  //                  for (int y = 0; y < schema.getFields().size(); y++) {
  //                    Field field = schema.getFields().get(y);
  //                    if (precompiledColumnNamePattern == null
  //                        || precompiledColumnNamePattern.matcher(field.getName()).matches()) {
  //                      adbcTableColumnsWriter.struct().start();
  //                      writeVarChar(
  //                          adbcTableColumnsWriter.struct().varChar("column_name"),
  // field.getName());
  //                      adbcTableColumnsWriter.struct().integer("ordinal_position").writeInt(y +
  // 1);
  //                      adbcTableColumnsWriter.struct().end();
  //                    }
  //                  }
  //                  adbcTableColumnsWriter.endList();
  //                }
  //
  //                adbcTablesStructWriter.end();
  //              }
  //            }
  //
  //            if (depth != AdbcConnection.GetObjectsDepth.CATALOGS) {
  //              adbcCatalogDbSchemaTablesWriter.endList();
  //              adbcCatalogDbSchemasStructWriter.end();
  //              adbcCatalogDbSchemasWriter.endList();
  //            }
  //          }
  //        }
  //      } catch (Exception e) {
  //        throw new RuntimeException(e);
  //      }
  //    }
  //
  //    root.setRowCount(catalogIndex);
  //    return root;
  //  }
  //
  //  private static List<FlightEndpoint> doRequest(FlightSqlClientWithCallOptions client,
  //                                                AdbcConnection.GetObjectsDepth depth, String
  // catalogPattern,
  //                                                String dbSchemaPattern, String tableNamePattern,
  // String[] tableTypes) {
  //
  //    final FlightInfo info;
  //    if (depth == AdbcConnection.GetObjectsDepth.CATALOGS) {
  //      info = client.getCatalogs();
  //    } else if (depth == AdbcConnection.GetObjectsDepth.DB_SCHEMAS) {
  //      info = client.getSchemas(catalogPattern, dbSchemaPattern);
  //    } else {
  //      info =
  //          client.getTables(
  //              catalogPattern,
  //              dbSchemaPattern,
  //              tableNamePattern,
  //              tableTypes == null ? null : Arrays.asList(tableTypes),
  //              depth == AdbcConnection.GetObjectsDepth.ALL);
  //    }
  //
  //    return info.getEndpoints();
  //  }
}
