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

package com.google.demo.analytics.model;

public class BigQueryUnitResult {

    private QueryUnit queryUnit;
    private double duration;
    private boolean hasError;
    private String message;

    public BigQueryUnitResult(QueryUnit queryUnit, double duration) {
        this.queryUnit = queryUnit;
        this.duration = duration;
    }

    public BigQueryUnitResult(QueryUnit queryUnit, boolean hasError, String message) {
        this.queryUnit = queryUnit;
        this.hasError = hasError;
        this.message = message;
    }

    public QueryUnit getQueryUnit() {
        return queryUnit;
    }

    public double getDuration() {
        return duration;
    }

    public boolean isHasError() {
        return hasError;
    }

    public String getMessage() {
        return message;
    }
}
