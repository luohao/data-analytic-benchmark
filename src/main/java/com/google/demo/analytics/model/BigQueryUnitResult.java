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

    public enum Status {
        SUCCESS,
        FAIL
    }

    private QueryUnit queryUnit;
    private String jobId;
    private Status status;
    private String duration;
    private String errorMessage;

    public static BigQueryUnitResult createSuccess(QueryUnit queryUnit, String jobId, String duration) {
        BigQueryUnitResult result = new BigQueryUnitResult();
        result.jobId = jobId;
        result.status = Status.SUCCESS;
        result.queryUnit = queryUnit;
        result.duration = duration;
        return result;
    }

    public static BigQueryUnitResult createFail(QueryUnit queryUnit, String jobId, String errorMessage) {
        BigQueryUnitResult result = new BigQueryUnitResult();
        result.queryUnit = queryUnit;
        result.jobId = jobId;
        result.status = Status.FAIL;
        result.errorMessage = errorMessage;
        return result;
    }

    public QueryUnit getQueryUnit() {
        return queryUnit;
    }

    public String getJobId() {
        return jobId;
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
}
