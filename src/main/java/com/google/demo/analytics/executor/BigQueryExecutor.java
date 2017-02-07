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

package com.google.demo.analytics.executor;

import com.google.cloud.bigquery.*;
import com.google.demo.analytics.model.BigQueryUnitResult;
import com.google.demo.analytics.model.QueryUnit;
import com.google.demo.analytics.util.StopWatch;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class BigQueryExecutor implements Executor<BigQueryUnitResult> {

    private Logger logger = LogManager.getLogger();

    @Override
    public BigQueryUnitResult execute(QueryUnit queryUnit) {
        BigQuery bigquery =
                new BigQueryOptions.DefaultBigqueryFactory().create(BigQueryOptions.getDefaultInstance());

        QueryRequest queryRequest =
                QueryRequest.newBuilder(queryUnit.getQuery())
                        .setMaxWaitTime(500L)
                        .setUseQueryCache(false)
                        .build();

        logger.log(Level.INFO, String.format("Using query = %s", queryUnit.getQuery()));

        StopWatch stopWatch = new StopWatch();
        QueryResponse response = bigquery.query(queryRequest);
        double duration = stopWatch.elapsedTime();

        if (response.hasErrors()) {
            return new BigQueryUnitResult(
                    queryUnit,
                    true,
                    response
                            .getExecutionErrors()
                            .stream()
                            .<String>map(err -> err.getMessage())
                            .collect(Collectors.joining("\n")));
        }

        JobStatistics statistics = bigquery.getJob(response.getJobId()).getStatistics();
        long creationTime = statistics.getCreationTime();
        long startTime = statistics.getStartTime();
        long endTime = statistics.getEndTime();

        QueryResult result = response.getResult();
        logger.log(Level.INFO, response);
        if(result != null) {
            Iterator<List<FieldValue>> iter = result.iterateAll();
            while (iter.hasNext()) {
                List<FieldValue> row = iter.next();
                logger.log(Level.INFO, row.stream().map(
                        val -> val.toString()).collect(Collectors.joining(",")));
            }
        }

        return new BigQueryUnitResult(queryUnit, duration);
    }
}
