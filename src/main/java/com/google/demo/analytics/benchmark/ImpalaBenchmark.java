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

import com.google.demo.analytics.executor.Executor;
import com.google.demo.analytics.executor.JDBCExecutor;
import com.google.demo.analytics.model.QueryPackage;
import com.google.demo.analytics.model.QueryUnitResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

public class ImpalaBenchmark extends Benchmark<QueryUnitResult> {

    private static final String driverName = "org.apache.hive.jdbc.HiveDriver";

    private Executor executor;

    private String user;
    private String password;
    private String connectionUrl;

    public ImpalaBenchmark(QueryPackage queryPackage) {
        super(queryPackage);
        parseHiveInput();
        this.executor = new JDBCExecutor(user, password, connectionUrl, driverName);
    }

    @Override
    protected Executor getExecutor() {
        return executor;
    }

    @Override
    protected void writeToOutput(List<QueryUnitResult> results, Path output) throws IOException {
        String headers = String.join(
                DELIMITER,
                "status",
                "label",
                "duration (ms)",
                "query",
                "error_messages");

        Files.write(output, Arrays.asList(headers), UTF_8, APPEND, CREATE);

        for(QueryUnitResult result : results) {
            Files.write(
                    output,
                    Arrays.asList(
                            String.join(
                                    DELIMITER,
                                    result.getStatus().toString(),
                                    result.getQueryUnit().getLabel(),
                                    result.getDuration(),
                                    result.getQueryUnit().getQuery(),
                                    result.getErrorMessage() == null ? "" : result.getErrorMessage())
                    ),
                    UTF_8,
                    APPEND);
        }
    }

    private void parseHiveInput() {
        Properties prop = new Properties();
        try {
            prop.load(HiveBenchmark.class.getClassLoader().getResourceAsStream("env.properties"));

            connectionUrl = prop.getProperty("impala.connection.url");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}