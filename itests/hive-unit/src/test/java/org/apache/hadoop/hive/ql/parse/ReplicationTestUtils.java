/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.WarehouseInstance;
import org.apache.hadoop.hive.ql.parse.repl.PathBuilder;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * ReplicationTestUtils - static helper functions for replication test
 */
public class ReplicationTestUtils {

    public static List<String> externalTableBasePathWithClause(String replExternalBase, WarehouseInstance replica)
            throws IOException, SemanticException {
        Path externalTableLocation = new Path(replExternalBase);
        DistributedFileSystem fileSystem = replica.miniDFSCluster.getFileSystem();
        externalTableLocation = PathBuilder.fullyQualifiedHDFSUri(externalTableLocation, fileSystem);
        fileSystem.mkdirs(externalTableLocation);

        // this is required since the same filesystem is used in both source and target
        return Arrays.asList(
                "'" + HiveConf.ConfVars.REPL_EXTERNAL_TABLE_BASE_DIR.varname + "'='"
                        + externalTableLocation.toString() + "'",
                "'distcp.options.pugpb'=''"
        );
    }

    public static void assertExternalFileInfo(WarehouseInstance primary,
                                              List<String> expected,
                                              Path externalTableInfoFile) throws IOException {
        DistributedFileSystem fileSystem = primary.miniDFSCluster.getFileSystem();
        Assert.assertTrue(fileSystem.exists(externalTableInfoFile));
        InputStream inputStream = fileSystem.open(externalTableInfoFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        Set<String> tableNames = new HashSet<>();
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            String[] components = line.split(",");
            Assert.assertEquals("The file should have tableName,base64encoded(data_location)",
                    2, components.length);
            tableNames.add(components[0]);
            Assert.assertTrue(components[1].length() > 0);
        }
        Assert.assertTrue(tableNames.containsAll(expected));
        reader.close();
    }
}