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

import com.google.cloud.WaitForOption;
import com.google.cloud.bigquery.*;
import com.google.common.base.Stopwatch;
import com.google.demo.analytics.model.BigQueryUnitResult;
import com.google.demo.analytics.model.QueryUnit;
import com.google.demo.analytics.util.StopWatch;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class BigQueryExecutor implements Callable<List<BigQueryUnitResult>> {

    private Logger logger = LogManager.getLogger();

    SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd_hh-mm-ss-SSS");

    private BigQuery bigquery;

    private QueryUnit queryUnit;
    private boolean useStopWatch;
    private boolean useQueryCache;

    public BigQueryExecutor(QueryUnit queryUnit, boolean useStopWatch, boolean useQueryCache) {
        this.queryUnit = queryUnit;
        this.useStopWatch = useStopWatch;
        this.useQueryCache = useQueryCache;
//        bigquery = new BigQueryOptions.DefaultBigqueryFactory().create(BigQueryOptions.getDefaultInstance());

        BigQueryOptions options = BigQueryOptions.newBuilder().setProjectId("da-core-research").build();
        bigquery = new BigQueryOptions.DefaultBigqueryFactory().create(options);
    }

    @Override
    public List<BigQueryUnitResult> call() throws Exception {
        List<BigQueryUnitResult> results = new ArrayList<>();
        for(int i = 0; i < queryUnit.getCount(); i++) {
            results.add(executeOnce(queryUnit));
        }
        return results;
    }

    private BigQueryUnitResult executeOnce(QueryUnit queryUnit) {
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(
                        queryUnit.getQuery())
                        // Use standard SQL syntax for queries.
                        // See: https://cloud.google.com/bigquery/sql-reference/
                        .setUseQueryCache(useQueryCache)
                        .setUseLegacySql(false)
                        .build();

        logger.log(Level.INFO, String.format(
                "%s - ID = %s - %s",
                Thread.currentThread().getName(),
                queryUnit.getId(),
                queryUnit.getDescription()));

            // Create a job ID so that we can safely retry.
            JobId jobId = JobId.of(UUID.randomUUID().toString());
            StopWatch stopWatch = new StopWatch();

            long duration = 0;

        try {
            Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

            // Wait for the query to complete.
            queryJob = queryJob.waitFor(
                    WaitForOption.checkEvery(1, TimeUnit.SECONDS),
                    WaitForOption.timeout(Long.MAX_VALUE, TimeUnit.SECONDS));

            // Check for errors
            if (queryJob == null) {
                error(jobId.getJob(), "Job no longer exists", stopWatch);
            } else if (queryJob.getStatus().getError() != null) {
                logger.log(Level.ERROR, String.format(
                        "%s - ID = %s - %s: %s",
                        Thread.currentThread().getName(),
                        queryUnit.getId(),
                        queryUnit.getDescription(),
                        queryJob.getStatus().getError().toString()));

                // You can also look at queryJob.getStatus().getExecutionErrors() for all
                // errors, not just the latest one.
                error(jobId.getJob(), queryJob.getStatus().getError().toString(), stopWatch);
            }

            duration = stopWatch.elapsedTime();

            if (!useStopWatch) {
                JobStatistics statistics = bigquery.getJob(jobId).getStatistics();
                long startTime = statistics.getStartTime();
                long endTime = statistics.getEndTime();
                duration = (endTime - startTime);
            }
        } catch(Throwable e) {
            logger.log(Level.ERROR, "Error in file: " + queryUnit.getDescription());
            e.printStackTrace();
            error(jobId.getJob(), e.getMessage(), stopWatch);
        }

        return BigQueryUnitResult.createSuccess(
                queryUnit,
                jobId.getJob(),
                String.valueOf(duration),
                sdf.format(new Date(stopWatch.getStart())).toString(),
                sdf.format(new Date(stopWatch.getEnd())).toString());
    }

    private BigQueryUnitResult error(String jobId, String errors, StopWatch stopwatch) {
        return BigQueryUnitResult.createFail(
                queryUnit,
                jobId,
                errors,
                new Date(stopwatch.getStart()).toString());
    }
}
