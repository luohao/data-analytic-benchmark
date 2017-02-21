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

import com.google.demo.analytics.model.QueryUnit;
import com.google.demo.analytics.model.QueryUnitResult;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class Benchmark<T extends QueryUnitResult> {

    public static final String DELIMITER = "|";

    private int threads = 1;

    private List<QueryUnit> queryUnits;

    public Benchmark(List<QueryUnit> queryUnits) {
        this.queryUnits = queryUnits;
        parseInput();
    }

    protected abstract Callable<List<T>> getExecutor(QueryUnit queryUnit);
    protected abstract void writeToOutput(List<T> results, Path output) throws IOException;
    public abstract String getFileOutputName();
    public abstract String getEngineName();
    protected abstract QueryUnit getCheckConnectionQuery();

    public void runQueries(Path output) throws IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(threads);

        List<Callable<List<T>>> callables = new ArrayList<>();
        for(QueryUnit queryUnit : queryUnits) {
            callables.add(getExecutor(queryUnit));
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

        writeToOutput(results, output);
        executorService.shutdown();
    }

    public void checkConnection() throws Exception {
        for(T result : getExecutor(getCheckConnectionQuery()).call()) {
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
        Properties prop = new Properties();
        try {
            prop.load(Benchmark.class.getClassLoader().getResourceAsStream("env.properties"));

            String threads = prop.getProperty("concurrent.threads");
            if(threads != null) {
                this.threads = Integer.parseInt(threads);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
