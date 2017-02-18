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
import com.google.demo.analytics.benchmark.HiveBenchmark;
import com.google.demo.analytics.benchmark.ImpalaBenchmark;
import com.google.demo.analytics.model.QueryPackage;
import com.google.demo.analytics.model.QueryUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class Main {

    private Logger logger = LogManager.getLogger();

    private List<QueryUnit> bigQueryUnits = new ArrayList<>();
    private List<QueryUnit> hiveQueryUnits = new ArrayList<>();
    private List<QueryUnit> impalaQueryUnits = new ArrayList<>();

    public static void main(String[] args) {
        Main main = new Main();

        main.start();
        main.cleanup();
    }

    private void start() {
        try {
            logger.log(Level.INFO, "Starting analytics");

            parseQueriesInput();

            bigquery();
            hive();
            impala();
        } catch(Throwable throwable) {
            logger.log(Level.ERROR, throwable);
            throwable.printStackTrace();
        }
    }

    private void bigquery() throws IOException {
        QueryPackage queryPackage = new QueryPackage(bigQueryUnits);

        Benchmark benchmark = new BigQueryBenchmark(queryPackage);
        String path = Main.class.getClassLoader().getResource("").getPath() + "bigquery-output.csv";
        Path output = Paths.get(path);

        logger.log(Level.INFO, "Running BQ benchmark");
        benchmark.runQueries(output);
        logger.log(Level.INFO, "Finished BQ benchmark");
    }

    private void hive() throws IOException {
        QueryPackage queryPackage = new QueryPackage(hiveQueryUnits);

        Benchmark benchmark = new HiveBenchmark(queryPackage);
        String path = Main.class.getClassLoader().getResource("").getPath() + "hive-output.csv";
        Path output = Paths.get(path);

        logger.log(Level.INFO, "Running Hive benchmark");
        benchmark.runQueries(output);
        logger.log(Level.INFO, "Finished Hive benchmark");
    }

    private void impala() throws IOException {
        QueryPackage queryPackage = new QueryPackage(impalaQueryUnits);

        Benchmark benchmark = new ImpalaBenchmark(queryPackage);
        String path = Main.class.getClassLoader().getResource("").getPath() + "impala-output.csv";
        Path output = Paths.get(path);

        logger.log(Level.INFO, "Running Impala benchmark");
        benchmark.runQueries(output);
        logger.log(Level.INFO, "Finished Impala benchmark");
    }

    private void parseQueriesInput() throws IOException {
        logger.log(Level.INFO, "Parsing input");
        LineIterator it = FileUtils.lineIterator(
                new File(Main.class.getClassLoader().getResource("queries.txt").getPath()),
                "UTF-8"
        );

        while(it.hasNext()) {
            String line = it.next();

            //Skip comments
            if(!line.startsWith("#")) {
                String[] keys = line.split("\\|");

                if(keys.length < 4) {
                    continue;
                }

                String label = keys[1];
                int count = isNumeric(keys[2]) ? Integer.valueOf(keys[2]) : 1;
                String query = keys[3];

                switch(keys[0]) {
                    case "bq":
                        bigQueryUnits.add(new QueryUnit(label, count, query));
                        break;
                    case "hive":
                        hiveQueryUnits.add(new QueryUnit(label, count, query));
                        break;
                    case "impala":
                        impalaQueryUnits.add(new QueryUnit(label,count, query));
                        break;
                }
            }
        }
        LineIterator.closeQuietly(it);
    }

    private void cleanup() {
        logger.log(Level.INFO, "Finished analytics");
    }

    private boolean isNumeric(String s) {
        return s.matches("[-+]?\\d*\\.?\\d+");
    }
}
