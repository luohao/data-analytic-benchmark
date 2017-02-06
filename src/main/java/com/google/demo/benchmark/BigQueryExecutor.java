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

package com.google.demo.benchmark;

import com.google.cloud.bigquery.*;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class BigQueryExecutor
{
    public static void main( String[] args )
    {
        BigQuery bigquery =
                new BigQueryOptions.DefaultBigqueryFactory().create(BigQueryOptions.getDefaultInstance());

        QueryRequest queryRequest =
                QueryRequest.newBuilder("select title from publicdata:samples.wikipedia limit 10")
                        .setMaxWaitTime(500L)
                        .setUseQueryCache(false)
                        .build();
        QueryResponse response = bigquery.query(queryRequest);

        if (response.hasErrors()) {
            throw new RuntimeException(
                    response
                            .getExecutionErrors()
                            .stream()
                            .<String>map(err -> err.getMessage())
                            .collect(Collectors.joining("\n")));
        }

        QueryResult result = response.getResult();
        Iterator<List<FieldValue>> iter = result.iterateAll();
        while (iter.hasNext()) {
            List<FieldValue> row = iter.next();
            System.out.println(row.stream().map(val -> val.toString()).collect(Collectors.joining(",")));
        }
    }
}
