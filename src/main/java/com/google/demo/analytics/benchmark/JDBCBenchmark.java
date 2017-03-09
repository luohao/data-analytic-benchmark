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

import com.google.demo.analytics.model.BigQueryUnitResult;
import com.google.demo.analytics.model.QueryPackage;
import com.google.demo.analytics.model.QueryUnit;
import com.google.demo.analytics.model.QueryUnitResult;
import com.google.demo.analytics.write.Writer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

public abstract class JDBCBenchmark extends Benchmark<QueryUnitResult> {

    public JDBCBenchmark(List<QueryPackage> queryPackages) {
        super(queryPackages);
    }

    @Override
    protected void writeToOutput(QueryPackage queryPackage, List<QueryUnitResult> results, Writer writer)
            throws IOException {
        List<String> baseHeaders = new ArrayList<>(Arrays.asList(
                "id",
                "query",
                "status",
                "duration_ms",
                "error_messages"
        ));

        baseHeaders.addAll(queryPackage.getKeys());
        String headers = String.join(DELIMITER, baseHeaders);

        writer.write(Arrays.asList(headers));

        for(QueryUnitResult result : results) {
            List<String> baseValues = new ArrayList<>(Arrays.asList(
                    result.getQueryUnit().getId(),
                    result.getQueryUnit().getQuery(),
                    result.getStatus().toString(),
                    result.getDuration(),
                    result.getErrorMessage() == null ? "" : result.getErrorMessage()
            ));

            baseValues.addAll(result.getQueryUnit().getValues());
            String values = String.join(DELIMITER, baseValues);

            writer.write(Arrays.asList(values));
        }

        writer.close();
    }
}
