/*
    Copyright 2017, Google, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
package com.google.demo.analytics.benchmark;

import com.google.demo.analytics.executor.JDBCExecutor;
import com.google.demo.analytics.model.QueryPackage;
import com.google.demo.analytics.model.QueryUnit;
import com.google.demo.analytics.model.QueryUnitResult;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

public class ExasolBenchmark extends JDBCBenchmark {

    private static final String driverName = "com.exasol.jdbc.EXADriver";

    public ExasolBenchmark(List<QueryPackage> queryPackages) {
        super(queryPackages);
    }

    @Override
    protected Callable<List<QueryUnitResult>> getExecutor(QueryUnit queryUnit, Properties props) {
        String user = props.getProperty("exasol.user");
        String password = props.getProperty("exasol.password") == null ? "" : props.getProperty("exasol.password");
        String connectionUrl = props.getProperty("exasol.connection.url");
        return new JDBCExecutor(queryUnit, user, password, connectionUrl, driverName);
    }

    @Override
    public String getFileOutputName() {
        return "exasol-output.csv";
    }

    @Override
    public String getEngineName() {
        return "Exasol";
    }

    @Override
    protected QueryUnit getCheckConnectionQuery(Properties props) {
        return new QueryUnit(
                "check",
                props.getProperty("exasol.connection.check"),
                1);
    }
}
