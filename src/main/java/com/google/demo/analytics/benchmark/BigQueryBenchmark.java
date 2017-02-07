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
import com.google.demo.analytics.executor.Executor;
import com.google.demo.analytics.model.BigQueryUnitResult;
import com.google.demo.analytics.model.QueryPackage;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class BigQueryBenchmark extends Benchmark<BigQueryUnitResult> {

    private Logger logger = LogManager.getLogger();

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
    protected void processResults(List<BigQueryUnitResult> results) {
        for(BigQueryUnitResult result : results) {
            logger.log(Level.INFO, String.format("Duration: %s", result.getDuration()));
        }
    }
}
