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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.StandardSchemas;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

final class ObjectMetadataBuilder {

    private final FlightSqlClientWithCallOptions client;
    private final VectorSchemaRoot root;
    private final VarCharVector adbcCatalogNames;
    private final UnionListWriter adbcCatalogDbSchemasWriter;
    private final BaseWriter.StructWriter adbcCatalogDbSchemasStructWriter;
    private final BaseWriter.ListWriter adbcCatalogDbSchemaTablesWriter;
    private final VarCharWriter adbcCatalogDbSchemaNameWriter;
    private final BaseWriter.StructWriter adbcTablesStructWriter;
    private final VarCharWriter adbcTableNameWriter;
    private final VarCharWriter adbcTableTypeWriter;
    private final BaseWriter.ListWriter adbcTableColumnsWriter;
    private final BufferAllocator allocator;
    private final AdbcConnection.GetObjectsDepth depth;
    private final String catalogPattern;
    private final String dbSchemaPattern;
    private final String tableNamePattern;
    private final String[] tableTypes;
    private final Pattern precompiledColumnNamePattern;

    ObjectMetadataBuilder(
            BufferAllocator allocator,
            FlightSqlClientWithCallOptions client,
            final AdbcConnection.GetObjectsDepth depth,
            final String catalogPattern,
            final String dbSchemaPattern,
            final String tableNamePattern,
            final String[] tableTypes,
            final String columnNamePattern) {
        this.allocator = allocator;
        this.client = client;
        this.depth = depth;
        this.catalogPattern = catalogPattern;
        this.dbSchemaPattern = dbSchemaPattern;
        this.tableNamePattern = tableNamePattern;
        this.precompiledColumnNamePattern = columnNamePattern != null ? Pattern.compile(
                Pattern.quote(columnNamePattern).replace("_", ".").replace("%", ".*")
        ) : null;
        this.tableTypes = tableTypes;
        this.root = VectorSchemaRoot.create(StandardSchemas.GET_OBJECTS_SCHEMA, allocator);
        this.adbcCatalogNames = (VarCharVector) root.getVector(0);
        this.adbcCatalogDbSchemasWriter = ((ListVector) root.getVector(1)).getWriter();
        this.adbcCatalogDbSchemasStructWriter = adbcCatalogDbSchemasWriter.struct();
        this.adbcCatalogDbSchemaTablesWriter =
                adbcCatalogDbSchemasStructWriter.list("db_schema_tables");
        this.adbcCatalogDbSchemaNameWriter = adbcCatalogDbSchemasStructWriter.varChar("db_schema_name");
        this.adbcTablesStructWriter = adbcCatalogDbSchemaTablesWriter.struct();
        this.adbcTableNameWriter = adbcTablesStructWriter.varChar("table_name");
        this.adbcTableTypeWriter = adbcTablesStructWriter.varChar("table_type");
        this.adbcTableColumnsWriter = adbcTablesStructWriter.list("table_columns");
    }

    private void writeVarChar(VarCharWriter writer, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        try (ArrowBuf tempBuf = allocator.buffer(bytes.length)) {
            tempBuf.setBytes(0, bytes, 0, bytes.length);
            writer.writeVarChar(0, bytes.length, tempBuf);
        }
    }

    /**
     * The caller must close the returned root.
     */
    VectorSchemaRoot build() throws AdbcException {
        // TODO Catalogs and schemas that don't contain tables are being left out
        FlightInfo info;
        if (depth == AdbcConnection.GetObjectsDepth.CATALOGS) {
            info = client.getCatalogs();
        } else if (depth == AdbcConnection.GetObjectsDepth.DB_SCHEMAS) {
            info = client.getSchemas(null, dbSchemaPattern);
        } else {
            info =
                    client.getTables(
                            null, // TODO pattern match later during processing
                            dbSchemaPattern,
                            tableNamePattern,
                            tableTypes == null ? null : Arrays.asList(tableTypes),
                            depth == AdbcConnection.GetObjectsDepth.ALL);
        }

        byte[] lastCatalogAdded = null;
        byte[] lastDbSchemaAdded = null;
        int catalogIndex = 0;

        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = client.getStream(endpoint.getTicket())) {
                while (stream.next()) {
                    try (VectorSchemaRoot res = stream.getRoot()) {
                        VarCharVector catalogVector = (VarCharVector) res.getVector(0);

                        for (int i = 0; i < res.getRowCount(); i++) {
                            byte[] catalog = catalogVector.get(i);

                            if (i == 0 || lastCatalogAdded != catalog) {
                                if (catalog == null) {
                                    adbcCatalogNames.setNull(catalogIndex);
                                } else {
                                    adbcCatalogNames.setSafe(catalogIndex, catalog);
                                }
                                if (depth == AdbcConnection.GetObjectsDepth.CATALOGS) {
                                    adbcCatalogDbSchemasWriter.writeNull();
                                } else {
                                    if (catalogIndex != 0) {
                                        adbcCatalogDbSchemasWriter.endList();
                                    }
                                    adbcCatalogDbSchemasWriter.startList();
                                    lastDbSchemaAdded = null;
                                }
                                catalogIndex++;
                                lastCatalogAdded = catalog;
                            }

                            if (depth != AdbcConnection.GetObjectsDepth.CATALOGS) {
                                VarCharVector dbSchemaVector = (VarCharVector) res.getVector(1);
                                byte[] dbSchema = dbSchemaVector.get(i);

                                if (!Arrays.equals(lastDbSchemaAdded, dbSchema)) {
                                    if (i != 0) {
                                        adbcCatalogDbSchemaTablesWriter.endList();
                                        adbcCatalogDbSchemasStructWriter.end();
                                    }
                                    adbcCatalogDbSchemasStructWriter.start();
                                    writeVarChar(
                                        adbcCatalogDbSchemaNameWriter, new String(dbSchema, StandardCharsets.UTF_8));
                                    if (depth == AdbcConnection.GetObjectsDepth.DB_SCHEMAS) {
                                        adbcCatalogDbSchemaTablesWriter.writeNull();
                                    } else {
                                        adbcCatalogDbSchemaTablesWriter.startList();
                                    }

                                    lastDbSchemaAdded = dbSchema;
                                }
                            }

                            if (depth != AdbcConnection.GetObjectsDepth.CATALOGS
                                && depth != AdbcConnection.GetObjectsDepth.DB_SCHEMAS) {
                                VarCharVector tableNameVector = (VarCharVector) res.getVector(2);
                                VarCharVector tableTypeVector = (VarCharVector) res.getVector(3);

                                adbcTablesStructWriter.start();
                                writeVarChar(
                                    adbcTableNameWriter, new String(tableNameVector.get(i), StandardCharsets.UTF_8));
                                writeVarChar(
                                    adbcTableTypeWriter, new String(tableTypeVector.get(i), StandardCharsets.UTF_8));

                                if (depth == AdbcConnection.GetObjectsDepth.ALL) {
                                    VarBinaryVector tableSchemaVector = (VarBinaryVector) res.getVector(4);
                                    Schema schema;

                                    try {
                                        schema =
                                            MessageSerializer.deserializeSchema(
                                                new ReadChannel(
                                                    Channels.newChannel(
                                                        new ByteArrayInputStream(tableSchemaVector.get(i)))));
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }

                                    adbcTableColumnsWriter.startList();

                                    for (int y = 0; y < schema.getFields().size(); y++) {
                                        Field field = schema.getFields().get(y);
                                        if (precompiledColumnNamePattern == null || precompiledColumnNamePattern.matcher(field.getName()).matches()) {
                                            adbcTableColumnsWriter.struct().start();
                                            writeVarChar(
                                                adbcTableColumnsWriter.struct().varChar("column_name"), field.getName());
                                            adbcTableColumnsWriter.struct().integer("ordinal_position").writeInt(y + 1);
                                            adbcTableColumnsWriter.struct().end();
                                        }
                                    }
                                    adbcTableColumnsWriter.endList();
                                }

                                adbcTablesStructWriter.end();
                            }
                        }

                        if (depth != AdbcConnection.GetObjectsDepth.CATALOGS) {
                            adbcCatalogDbSchemaTablesWriter.endList();
                            adbcCatalogDbSchemasStructWriter.end();
                            adbcCatalogDbSchemasWriter.endList();
                        }
                    }
                }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
        }

        root.setRowCount(catalogIndex);
        return root;
    }
}
