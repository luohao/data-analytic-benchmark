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

public class QueryUnit {

    private String id;
    private String query;
    private int count;

    public QueryUnit(String id, String query, int count) {
        this.id = id;
        this.query = query;
        this.count = count;
    }

    public String getId() {
        return id;
    }

    public String getQuery() {
        return query;
    }

    public int getCount() {
        return count;
    }
}
