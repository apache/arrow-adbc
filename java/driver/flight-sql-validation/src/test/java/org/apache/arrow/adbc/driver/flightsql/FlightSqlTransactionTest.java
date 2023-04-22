package org.apache.arrow.adbc.driver.flightsql;

import org.apache.arrow.adbc.driver.testsuite.AbstractTransactionTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;

public class FlightSqlTransactionTest extends AbstractTransactionTest {

  @BeforeAll
  public static void beforeAll() {
    quirks = new FlightSqlQuirks();
  }

  @Override
  @Disabled("Not yet implemented")
  public void enableAutoCommitAlsoCommits() throws Exception {}

  @Override
  @Disabled("Not yet implemented")
  public void commit() throws Exception {}

  @Override
  @Disabled("Not yet implemented")
  public void rollback() throws Exception {}

  @Override
  @Disabled("Not yet implemented")
  public void autoCommitByDefault() throws Exception {}

  @Override
  @Disabled("Not yet implemented")
  public void toggleAutoCommit() throws Exception {}
}
