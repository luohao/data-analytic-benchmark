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

import com.google.demo.analytics.executor.Executor;
import com.google.demo.analytics.model.QueryPackage;
import com.google.demo.analytics.model.QueryUnit;
import com.google.demo.analytics.model.BigQueryUnitResult;

import java.util.ArrayList;
import java.util.List;

public abstract class Benchmark<T> {

    private QueryPackage queryPackage;

    protected abstract Executor<T> getExecutor();

    protected abstract void processResults(List<T> results);

    public Benchmark(QueryPackage queryPackage) {
        this.queryPackage = queryPackage;
    }

    public void runQueries() {
        List<T> results = new ArrayList<>();

        for(QueryUnit queryUnit : queryPackage.getQueryUnits()) {
            T result = getExecutor().execute(queryUnit);
            results.add(result);
        }

        processResults(results);
    }
}
