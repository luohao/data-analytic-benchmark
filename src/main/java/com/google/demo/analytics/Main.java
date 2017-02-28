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

import com.google.demo.analytics.benchmark.*;
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
    private List<QueryUnit> exasolQueryUnits = new ArrayList<>();

    public static void main(String[] args) {
        Main main = new Main();

        main.start();
        main.cleanup();
    }

    private void start() {
        try {
            logger.log(Level.INFO, "Starting analytics");

            parseQueriesInput();

            List<Benchmark> benchmarks = new ArrayList<>();
            benchmarks.add(new BigQueryBenchmark(bigQueryUnits));
            benchmarks.add(new HiveBenchmark(hiveQueryUnits));
            benchmarks.add(new ImpalaBenchmark(impalaQueryUnits));
            benchmarks.add(new ExasolBenchmark(exasolQueryUnits));

            checkConnections(benchmarks);

            runBenchmarks(benchmarks);
        } catch(Throwable throwable) {
            logger.log(Level.ERROR, throwable);
            throwable.printStackTrace();
        }
    }

    private void checkConnections(List<Benchmark> benchmarks) throws Exception {
        for(Benchmark benchmark : benchmarks) {
            benchmark.checkConnection();
            logger.log(Level.INFO, String.format("Checking connection for %s - OK", benchmark.getEngineName()));
        }
    }

    private void runBenchmarks(List<Benchmark> benchmarks) throws IOException {
        for(Benchmark benchmark : benchmarks) {
            String path = Main.class.getClassLoader().getResource("").getPath() + benchmark.getFileOutputName();
            Path output = Paths.get(path);

            logger.log(Level.INFO, String.format("Running %s benchmark", benchmark.getEngineName()));
            benchmark.runQueries(output);
            logger.log(Level.INFO, String.format("Finished %s benchmark", benchmark.getEngineName()));
        }
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
                        impalaQueryUnits.add(new QueryUnit(label, count, query));
                        break;
                    case "exasol":
                        exasolQueryUnits.add(new QueryUnit(label, count, query));
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
