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
package com.google.demo.analytics.executor;

import com.google.demo.analytics.model.QueryUnit;
import com.google.demo.analytics.model.QueryUnitResult;
import com.google.demo.analytics.util.StopWatch;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class JDBCExecutor implements Callable<List<QueryUnitResult>> {

    private Logger logger = LogManager.getLogger();

    private QueryUnit queryUnit;

    private String user;
    private String password;
    private String connectionUrl;
    private String driverName;

    public JDBCExecutor(QueryUnit queryUnit, String user, String password, String connectionUrl, String driverName) {
        this.queryUnit = queryUnit;
        this.user = user;
        this.password = password;
        this.connectionUrl = connectionUrl;
        this.driverName = driverName;
    }

    @Override
    public List<QueryUnitResult> call() throws Exception {
        logger.log(Level.INFO, Thread.currentThread().getName());

        List<QueryUnitResult> results = new ArrayList<>();
        for(int i = 0; i < queryUnit.getCount(); i++) {
            results.add(executeOnce(queryUnit));
        }
        return results;
    }

    private QueryUnitResult executeOnce(QueryUnit queryUnit) {
        Connection cnct = null;
        try {
            Class.forName(driverName);
            cnct = DriverManager.getConnection(connectionUrl, user, password);
            Statement stmt = cnct.createStatement();

            StopWatch stopWatch = new StopWatch();
            ResultSet res = stmt.executeQuery(queryUnit.getQuery());
            long duration = stopWatch.elapsedTime();
//            while (res.next()) {
//                logger.log(Level.INFO, res.getString(1));
//            }

            return QueryUnitResult.createSuccess(queryUnit, String.valueOf(duration));
        } catch (SQLException | ClassNotFoundException e) {
            return QueryUnitResult.createFail(queryUnit, e.getMessage());
        } finally {
            try {
                if(cnct != null) {
                    cnct.close();
                }
            } catch (SQLException e) {
                logger.log(Level.ERROR, "Error closing the JDBC connection", e);
            }
        }
    }
}
