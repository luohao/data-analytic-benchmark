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

public class QueryUnitResult {

    public enum Status {
        SUCCESS,
        FAIL
    }

    private QueryUnit queryUnit;
    private Status status;
    private String duration;
    private String errorMessage;
    private String start;
    private String end;

    public QueryUnitResult(
            QueryUnit queryUnit, Status status, String duration, String errorMessage, String start, String end) {
        this.queryUnit = queryUnit;
        this.status = status;
        this.duration = duration;
        this.errorMessage = errorMessage;
        this.start = start;
        this.end = end;
    }

    public static QueryUnitResult createSuccess(QueryUnit queryUnit, String duration, String start, String end) {
        return new QueryUnitResult(queryUnit, Status.SUCCESS, duration, null, start, end);
    }

    public static QueryUnitResult createFail(QueryUnit queryUnit, String errorMessage, String start) {
        return new QueryUnitResult(queryUnit, Status.FAIL, null, errorMessage, start, null);
    }

    public QueryUnit getQueryUnit() {
        return queryUnit;
    }

    public Status getStatus() {
        return status;
    }

    public String getDuration() {
        return duration;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getStart() {
        return start;
    }

    public String getEnd() {
        return end;
    }
}
