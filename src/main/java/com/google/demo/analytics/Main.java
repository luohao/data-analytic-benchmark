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

package com.google.demo.analytics;

import com.google.demo.analytics.benchmark.Benchmark;
import com.google.demo.analytics.benchmark.BigQueryBenchmark;
import com.google.demo.analytics.model.QueryPackage;
import com.google.demo.analytics.model.QueryUnit;
import com.google.demo.analytics.model.BigQueryUnitResult;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;

public class Main {

    private Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Main main = new Main();
        main.start();
        main.cleanup();
    }

    private void start() {
        logger.log(Level.INFO, "Starting analytics");

        parseInput();

        QueryUnit queryUnit = new QueryUnit(
                "small",
                1,
                "select repo_name from [bigquery-public-data:github_repos.licenses] where license = \"apache-2.0\" limit 10;");
        QueryPackage queryPackage = new QueryPackage(Arrays.asList(queryUnit));

        Benchmark benchmark = new BigQueryBenchmark(queryPackage);
        benchmark.runQueries();
    }

    private void parseInput() {

    }

    private void cleanup() {
        logger.log(Level.INFO, "Finished analytics");
    }
}
