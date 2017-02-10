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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class BigQueryExecutor implements Executor<BigQueryUnitResult> {

    private Logger logger = LogManager.getLogger();

    private BigQuery bigquery =
            new BigQueryOptions.DefaultBigqueryFactory().create(BigQueryOptions.getDefaultInstance());

    @Override
    public List<BigQueryUnitResult> execute(QueryUnit queryUnit) {
        List<BigQueryUnitResult> results = new ArrayList<>();
        for(int i = 0; i < queryUnit.getCount(); i++) {
            results.add(executeOnce(queryUnit));
        }
        return results;
    }

    private BigQueryUnitResult executeOnce(QueryUnit queryUnit) {
        QueryRequest request = QueryRequest.newBuilder(queryUnit.getQuery())
                .setUseQueryCache(false)
                .build();

//        StopWatch stopWatch = new StopWatch();
        QueryResponse response = bigquery.query(request);
        // Wait for things to finish
        while (!response.jobCompleted()) {
            try {
                logger.log(Level.INFO, "NOT DONE");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.log(Level.ERROR, e);
            }
            response = bigquery.getQueryResults(response.getJobId());
        }
//        double duration = stopWatch.elapsedTime();

        if (response.hasErrors()) {
            String errors = response.getExecutionErrors()
                    .stream()
                    .map(i -> i.toString()).collect(Collectors.joining(","));
            return BigQueryUnitResult.createFail(queryUnit, response.getJobId().toString(), errors);
        }

        JobStatistics statistics = bigquery.getJob(response.getJobId()).getStatistics();
        long creationTime = statistics.getCreationTime();
        long startTime = statistics.getStartTime();
        long endTime = statistics.getEndTime();

        double duration = (endTime - startTime);

//        QueryResult result = response.getResult();
//        if(result != null) {
//            Iterator<List<FieldValue>> iter = result.iterateAll();
//            while (iter.hasNext()) {
//                List<FieldValue> row = iter.next();
//                logger.log(Level.INFO, row.stream().map(
//                        val -> val.toString()).collect(Collectors.joining(",")));
//            }
//        }

        return BigQueryUnitResult.createSuccess(queryUnit, response.getJobId().toString(), String.valueOf(duration));
    }
}
