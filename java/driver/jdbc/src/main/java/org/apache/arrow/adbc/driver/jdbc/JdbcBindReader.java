package org.apache.arrow.adbc.driver.jdbc;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.adbc.driver.jdbc.util.JdbcParameterBinder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

/** An Arrow reader that binds parameters. */
final class JdbcBindReader extends ArrowReader {
  private final PreparedStatement statement;
  private final JdbcParameterBinder binder;
  private ResultSet currentResultSet;
  private ArrowVectorIterator currentSource;

  JdbcBindReader(
      BufferAllocator allocator, PreparedStatement statement, VectorSchemaRoot bindParameters) {
    super(allocator);
    this.statement = statement;
    this.binder = JdbcParameterBinder.builder(statement, bindParameters).bindAll().build();
  }

  @Override
  public boolean loadNextBatch() throws IOException {
    if (currentSource == null || !currentSource.hasNext()) {
      if (!advance()) {
        return false;
      }
    }

    final VectorSchemaRoot root = currentSource.next();
    try (final ArrowRecordBatch batch = new VectorUnloader(root).getRecordBatch()) {
      loadRecordBatch(batch);
    }
    return true;
  }

  @Override
  public long bytesRead() {
    return 0;
  }

  @Override
  protected void closeReadSource() throws IOException {
    try {
      // Do not close PreparedStatement so we can reuse it
      currentResultSet.close();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected Schema readSchema() throws IOException {
    try {
      if (!advance()) {
        throw new IOException("Parameter set is empty!");
      }
      return JdbcToArrowUtils.jdbcToArrowSchema(
          currentResultSet.getMetaData(), JdbcToArrowUtils.getUtcCalendar());
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  private boolean advance() throws IOException {
    try {
      if (binder.next()) {
        if (currentResultSet != null) {
          currentSource.close();
          currentResultSet.close();
        }
        currentResultSet = statement.executeQuery();
        currentSource = JdbcToArrow.sqlToArrowVectorIterator(currentResultSet, allocator);
        return true;
      }
    } catch (SQLException e) {
      throw new IOException(e);
    }
    return false;
  }
}
