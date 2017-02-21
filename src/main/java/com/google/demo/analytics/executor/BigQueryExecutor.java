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
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class BigQueryExecutor implements Callable<List<BigQueryUnitResult>> {

    private Logger logger = LogManager.getLogger();

    private BigQuery bigquery =
            new BigQueryOptions.DefaultBigqueryFactory().create(BigQueryOptions.getDefaultInstance());

    private QueryUnit queryUnit;

    public BigQueryExecutor(QueryUnit queryUnit) {
        this.queryUnit = queryUnit;
    }

    @Override
    public List<BigQueryUnitResult> call() throws Exception {
        logger.log(Level.INFO, Thread.currentThread().getName());

        List<BigQueryUnitResult> results = new ArrayList<>();
        for(int i = 0; i < queryUnit.getCount(); i++) {
            results.add(executeOnce(queryUnit));
        }
        return results;
    }

    private BigQueryUnitResult executeOnce(QueryUnit queryUnit) {
        QueryRequest request = QueryRequest.newBuilder(queryUnit.getQuery())
                .setUseQueryCache(true)
                .build();

//        StopWatch stopWatch = new StopWatch();
        QueryResponse response = bigquery.query(request);
        // Wait for things to finish
        while (!response.jobCompleted()) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                logger.log(Level.ERROR, e);
            }
            response = bigquery.getQueryResults(response.getJobId());
        }
//        double duration = stopWatch.elapsedTime();

        JobStatistics statistics = bigquery.getJob(response.getJobId()).getStatistics();
        long creationTime = statistics.getCreationTime();
        long startTime = statistics.getStartTime();
        long endTime = statistics.getEndTime();

        if (response.hasErrors()) {
            String errors = response.getExecutionErrors()
                    .stream()
                    .map(i -> i.toString()).collect(Collectors.joining(","));
            return BigQueryUnitResult.createFail(
                    queryUnit,
                    response.getJobId().toString(),
                    errors,
                    new Date(creationTime).toString());
        }

        long duration = (endTime - startTime);

//        QueryResult result = response.getResult();
//        if(result != null) {
//            Iterator<List<FieldValue>> iter = result.iterateAll();
//            while (iter.hasNext()) {
//                List<FieldValue> row = iter.next();
//                logger.log(Level.INFO, row.stream().map(
//                        val -> val.toString()).collect(Collectors.joining(",")));
//            }
//        }

        return BigQueryUnitResult.createSuccess(
                queryUnit,
                response.getJobId().getJob(),
                String.valueOf(duration),
                new Date(creationTime).toString());
    }
}
