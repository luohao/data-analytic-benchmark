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
package com.google.demo.analytics.write;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSWriter implements Writer {

    private Logger logger = LogManager.getLogger();

//    private FileSystem hdfs;
//    private BufferedWriter br;
    private FSDataOutputStream out;

    public HDFSWriter(String hdfsHost, String hdfsPath) throws URISyntaxException, IOException {
        logger.log(Level.INFO, String.format("Creating writer for %s", hdfsPath));
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", hdfsHost);
        System.out.println("Connecting to -- "+configuration.get("fs.defaultFS"));

        FileSystem fileSystem = FileSystem.get(configuration);
        out = fileSystem.create(new Path(hdfsPath));


//        hdfs = FileSystem.get(new URI(hdfsHost), configuration);
//        Path file = new Path(hdfsPath);
//        OutputStream os = hdfs.create(file);
//        br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8") );
    }

    @Override
    public void write(Iterable<? extends CharSequence> line) throws IOException {
        out.writeUTF(line.toString());
    }

    @Override
    public void close() throws IOException {
        out.close();
//        hdfs.close();
    }
}
