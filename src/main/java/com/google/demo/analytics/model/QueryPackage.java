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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class QueryPackage {

    private String engine;
    private String description;
    private List<String> keys;
    private List<QueryUnit> queryUnits = new ArrayList<>();

    public QueryPackage(String engine, String description, List<String> keys, List<QueryUnit> queryUnits) {
        this.engine = engine;
        this.description = description;
        this.keys = keys;
        this.queryUnits = queryUnits;
    }

    public String getEngine() {
        return engine;
    }

    public String getDescription() {
        return description;
    }

    public List<String> getKeys() {
        return keys;
    }

    public List<QueryUnit> getQueryUnits() {
        return queryUnits;
    }
}
