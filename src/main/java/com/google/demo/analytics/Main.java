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
import com.google.demo.analytics.model.QueryPackage;
import com.google.demo.analytics.model.QueryUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    private Logger logger = LogManager.getLogger();

    private static final String TEXT_EXTENSION = ".txt";

    private List<QueryPackage> bigQueryPackages = new ArrayList<>();
    private List<QueryPackage> hiveQueryPackages = new ArrayList<>();
    private List<QueryPackage> impalaQueryPackages = new ArrayList<>();
    private List<QueryPackage> exasolQueryPackages = new ArrayList<>();

    private Map<String, List<String>> platformToKeys = new HashMap<>();

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
            benchmarks.add(new BigQueryBenchmark(platformToKeys.get(BigQueryBenchmark.ENGINE_NAME), bigQueryPackages));
            benchmarks.add(new HiveBenchmark(platformToKeys.get(HiveBenchmark.ENGINE_NAME), hiveQueryPackages));
            benchmarks.add(new ImpalaBenchmark(platformToKeys.get(ImpalaBenchmark.ENGINE_NAME), impalaQueryPackages));
            benchmarks.add(new ExasolBenchmark(platformToKeys.get(ExasolBenchmark.ENGINE_NAME), exasolQueryPackages));

            runBenchmarks(checkConnections(benchmarks));
        } catch(Throwable throwable) {
            logger.log(Level.ERROR, throwable);
            throwable.printStackTrace();
        }
    }

    private List<Benchmark> checkConnections(List<Benchmark> benchmarks) throws Exception {
        List<Benchmark> results = new ArrayList<>();
        for(Benchmark benchmark : benchmarks) {
            try {
                benchmark.checkConnection();
                results.add(benchmark);
                logger.log(Level.INFO, String.format("Checking connection for %s - OK", benchmark.getEngineName()));
            } catch(Throwable throwable) {
                logger.log(Level.ERROR, String.format(
                        "Checking connection for %s - ERROR - %s",
                        benchmark.getEngineName(),
                        throwable.getMessage()));
            }
        }
        return results;
    }

    private void runBenchmarks(List<Benchmark> benchmarks) throws IOException, URISyntaxException {
        for(Benchmark benchmark : benchmarks) {
            benchmark.runQueries();
        }
    }

    private void parseQueriesInput() throws IOException {
        logger.log(Level.INFO, "Parsing input queries");
        String path = Main.class.getClassLoader().getResource("").getPath();
        Path resources = Paths.get(path);


        Files.list(resources)
                .filter(Files::isRegularFile)
                .forEach(file -> {
                    if(file.getFileName().toString().startsWith(BigQueryBenchmark.ENGINE_NAME)
                            && file.getFileName().toString().endsWith(TEXT_EXTENSION)) {
                        bigQueryPackages.add(getQueryPackage(file, BigQueryBenchmark.ENGINE_NAME));
                    } else if(file.getFileName().toString().startsWith(HiveBenchmark.ENGINE_NAME)
                            && file.getFileName().toString().endsWith(TEXT_EXTENSION)) {
                        hiveQueryPackages.add(getQueryPackage(file, HiveBenchmark.ENGINE_NAME));
                    } else if(file.getFileName().toString().startsWith(ImpalaBenchmark.ENGINE_NAME)
                            && file.getFileName().toString().endsWith(TEXT_EXTENSION)) {
                        impalaQueryPackages.add(getQueryPackage(file, ImpalaBenchmark.ENGINE_NAME));
                    } else if(file.getFileName().toString().startsWith(ExasolBenchmark.ENGINE_NAME)
                            && file.getFileName().toString().endsWith(TEXT_EXTENSION)) {
                        exasolQueryPackages.add(getQueryPackage(file, ExasolBenchmark.ENGINE_NAME));
                    }
                });
    }

    private QueryPackage getQueryPackage(Path file, String prefix) {
        logger.log(Level.INFO, String.format("Parsing file %s", file.getFileName()));
        LineIterator it = null;
        try {
            it = FileUtils.lineIterator(
                    new File(file.toUri()),
                    "UTF-8"
            );

            List<String> keys = new ArrayList<>();
            List<QueryUnit> queryUnits = new ArrayList<>();

            String baseName = FilenameUtils.getBaseName(file.getFileName().toString());
            String noPrefix = baseName.substring(prefix.length() + 1);
            String description = noPrefix.substring(0, noPrefix.length());

            while(it.hasNext()) {
                String line = it.next();

                //Skip comments
                if(!line.startsWith("#")) {
                    String[] columns = line.split("\\|");

                    if(columns.length < 3) {
                        continue;
                    }

                    // Headers line
                    if("id".equals(columns[0])) {
                        if(platformToKeys.containsKey(prefix)) {
                            continue;
                        }

                        for(int i = 3; i < columns.length; i++) {
                            keys.add(columns[i]);
                        }

                        platformToKeys.put(prefix, keys);
                        continue;
                    }

                    String id = columns[0];
                    String query = columns[1];
                    int count = isNumeric(columns[2]) ? Integer.valueOf(columns[2]) : 1;

                    List<String> values = new ArrayList<>();
                    for(int i = 3; i < columns.length; i++) {
                        values.add(columns[i]);
                    }

                    queryUnits.add(new QueryUnit(id, prefix, description, query, count, values));
                }
            }

            return new QueryPackage(prefix, description, queryUnits);
        } catch (Throwable e) {
            logger.log(Level.ERROR, String.format("Error parsing file %s", file));
            throw new RuntimeException(e);
        } finally {
            LineIterator.closeQuietly(it);
        }
    }

    private void cleanup() {
        logger.log(Level.INFO, "Finished analytics");
    }

    private boolean isNumeric(String s) {
        return s.matches("[-+]?\\d*\\.?\\d+");
    }
}
