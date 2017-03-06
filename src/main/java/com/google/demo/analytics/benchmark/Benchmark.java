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

import com.google.demo.analytics.model.QueryPackage;
import com.google.demo.analytics.model.QueryUnit;
import com.google.demo.analytics.model.QueryUnitResult;
import com.google.demo.analytics.write.DefaultWriter;
import com.google.demo.analytics.write.Writer;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class Benchmark<T extends QueryUnitResult> {

    private Logger logger = LogManager.getLogger();

    public static final String DELIMITER = "|";

    private int threads = 1;
    private Properties props;

    private List<QueryPackage> queryPackages;

    public Benchmark(List<QueryPackage> queryPackages) {
        this.queryPackages = queryPackages;
        parseInput();
    }

    protected abstract Callable<List<T>> getExecutor(QueryUnit queryUnit, Properties props);
    protected abstract void writeToOutput(List<T> results, Writer writer) throws IOException;
    public abstract String getFileOutputName();
    public abstract String getEngineName();
    protected abstract QueryUnit getCheckConnectionQuery(Properties props);

    public void runQueries() throws IOException {
        if(queryPackages.isEmpty()) {
            logger.log(Level.INFO, String.format("Not queries to run for %s", getEngineName()));
            return;
        }

        logger.log(Level.INFO, String.format("Running %s benchmark", getEngineName()));
        ExecutorService executorService = Executors.newFixedThreadPool(threads);

        for(QueryPackage queryPackage : queryPackages) {
            List<Callable<List<T>>> callables = new ArrayList<>();

            for(QueryUnit queryUnit : queryPackage.getQueryUnits()) {
                callables.add(getExecutor(queryUnit, props));
            }

            List<T> results = new ArrayList<>();
            try {
                executorService.invokeAll(callables)
                        .stream()
                        .map(future -> {
                            try {
                                return future.get();
                            }
                            catch (Exception e) {
                                throw new IllegalStateException(e);
                            }
                        })
                        .forEach(i -> results.addAll(i));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            writeToOutput(results, new DefaultWriter(queryPackage.getQueryFile()));
        }

        executorService.shutdown();

        logger.log(Level.INFO, String.format("Finished %s benchmark", getEngineName()));
    }

    public void checkConnection() throws Exception {
        for(T result : getExecutor(getCheckConnectionQuery(props), props).call()) {
            if(QueryUnitResult.Status.FAIL.equals(result.getStatus())) {
                throw new RuntimeException(
                        String.format(
                                "Error checking the connection for %s. Error: %s",
                                getEngineName(),
                                result.getErrorMessage()));
            }
        }
    }

    private void parseInput() {
        props = new Properties();
        try {
            props.load(Benchmark.class.getClassLoader().getResourceAsStream("env.properties"));

            String threads = props.getProperty("concurrent.threads");
            if(threads != null) {
                this.threads = Integer.parseInt(threads);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
