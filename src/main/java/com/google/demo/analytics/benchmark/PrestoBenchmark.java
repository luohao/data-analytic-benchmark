package com.google.demo.analytics.benchmark;

import com.google.demo.analytics.executor.JDBCExecutor;
import com.google.demo.analytics.model.QueryPackage;
import com.google.demo.analytics.model.QueryUnit;
import com.google.demo.analytics.model.QueryUnitResult;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

public class PrestoBenchmark extends JDBCBenchmark {
  public final static String ENGINE_NAME = "presto";
  private static final String driverName = "com.facebook.presto.jdbc.PrestoDriver";

  public PrestoBenchmark(List<String> keys, List<QueryPackage> queryPackages) {
    super(keys, queryPackages);
  }

  @Override
  protected Callable<List<QueryUnitResult>> getExecutor(QueryUnit queryUnit, Properties props) {
    String user = props.getProperty("presto.user");
    String password = props.getProperty("presto.password") == null ? "" : props.getProperty("presto.password");
    String connectionUrl = props.getProperty("presto.connection.url");
    return new JDBCExecutor(queryUnit, user, password, connectionUrl, driverName);
  }

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  protected QueryUnit getCheckConnectionQuery(Properties props) {
    return new QueryUnit(
        "check",
        getEngineName(),
        "check-connection",
        props.getProperty("presto.connection.check"),
        1);
  }
}
