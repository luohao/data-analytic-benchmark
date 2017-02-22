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

import com.google.demo.analytics.executor.BigQueryExecutor;
import com.google.demo.analytics.model.BigQueryUnitResult;
import com.google.demo.analytics.model.QueryUnit;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

public class BigQueryBenchmark extends Benchmark<BigQueryUnitResult> {

    private boolean useStopwatch;

    public BigQueryBenchmark(List<QueryUnit> queryUnits) {
        super(queryUnits);
        parseInput();
    }

    @Override
    protected Callable<List<BigQueryUnitResult>> getExecutor(QueryUnit queryUnit) {
        return new BigQueryExecutor(queryUnit, useStopwatch);
    }

    @Override
    public String getFileOutputName() {
        return "bigquery-output.csv";
    }

    @Override
    public String getEngineName() {
        return "BQ";
    }

    @Override
    protected QueryUnit getCheckConnectionQuery() {
        return new QueryUnit(
                "check",
                1,
                "select repo_name from [gcp-rocco:examples.licenses] limit 10");
    }

    @Override
    protected void writeToOutput(List<BigQueryUnitResult> results, Path output) throws IOException {
        String headers = String.join(
                DELIMITER,
                "job_id",
                "status",
                "label",
                "creation_time",
                "duration_ms",
                "query",
                "error_messages");

        Files.write(output, Arrays.asList(headers), UTF_8, APPEND, CREATE);

        for(BigQueryUnitResult result : results) {
            Files.write(
                    output,
                    Arrays.asList(
                            String.join(
                                    DELIMITER,
                                    result.getJobId(),
                                    result.getStatus().toString(),
                                    result.getQueryUnit().getLabel(),
                                    result.getCreationTime(),
                                    result.getDuration(),
                                    result.getQueryUnit().getQuery(),
                                    result.getErrorMessage() == null ? "" : result.getErrorMessage())
                    ),
                    UTF_8,
                    APPEND);
        }
    }

    private void parseInput() {
        Properties prop = new Properties();
        try {
            prop.load(BigQueryBenchmark.class.getClassLoader().getResourceAsStream("env.properties"));

            String useStopwatch = prop.getProperty("bq.stopwatch");
            this.useStopwatch = Boolean.parseBoolean(useStopwatch);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
