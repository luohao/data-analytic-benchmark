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

import com.google.demo.analytics.model.QueryUnit;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class ImpalaBenchmark extends JDBCBenchmark {

    private static final String driverName = "org.apache.hive.jdbc.HiveDriver";

    private String user;
    private String password;
    private String connectionUrl;

    public ImpalaBenchmark(List<QueryUnit> queryUnits) {
        super(queryUnits);
        parseInput();
    }

    @Override
    public String getFileOutputName() {
        return "impala-output.csv";
    }

    @Override
    public String getEngineName() {
        return "Impala";
    }

    @Override
    protected String getUser() {
        return user;
    }

    @Override
    protected String getPassword() {
        return password;
    }

    @Override
    protected String getConnectionUrl() {
        return connectionUrl;
    }

    @Override
    protected String getDriverName() {
        return driverName;
    }

    @Override
    protected QueryUnit getCheckConnectionQuery() {
        return new QueryUnit(
                "check",
                1,
                "show tables");
    }

    private void parseInput() {
        Properties prop = new Properties();
        try {
            prop.load(ImpalaBenchmark.class.getClassLoader().getResourceAsStream("env.properties"));

            user = prop.getProperty("impala.user");
            password = prop.getProperty("impala.password") == null ? "" : prop.getProperty("impala.password");
            connectionUrl = prop.getProperty("impala.connection.url");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}