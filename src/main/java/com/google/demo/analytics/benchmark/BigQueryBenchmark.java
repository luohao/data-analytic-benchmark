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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

import com.google.demo.analytics.executor.BigQueryExecutor;
import com.google.demo.analytics.executor.Executor;
import com.google.demo.analytics.model.BigQueryUnitResult;
import com.google.demo.analytics.model.QueryPackage;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class BigQueryBenchmark extends Benchmark<BigQueryUnitResult> {

    private Logger logger = LogManager.getLogger();

    private static final String DELIMITER = "|";

    private Executor executor;

    public BigQueryBenchmark(QueryPackage queryPackage) {
        super(queryPackage);
        this.executor = new BigQueryExecutor();
    }

    @Override
    protected Executor getExecutor() {
        return executor;
    }

    @Override
    protected void writeToOutput(List<BigQueryUnitResult> results, Path output) throws IOException {
        String headers = String.join(
                DELIMITER,
                "job_id",
                "status",
                "label",
                "duration (ms)",
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
                                    result.getDuration(),
                                    result.getQueryUnit().getQuery(),
                                    result.getErrorMessage() == null ? "" : result.getErrorMessage())
                    ),
                    UTF_8,
                    APPEND);
        }
    }
}
