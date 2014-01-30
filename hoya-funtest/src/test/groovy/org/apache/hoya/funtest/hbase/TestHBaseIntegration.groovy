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

package org.apache.hoya.funtest.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.IntegrationTestIngest
import org.apache.hadoop.hbase.trace.IntegrationTestSendTraceRequests
import org.apache.hadoop.hbase.test.MetricsAssertHelper
import org.apache.hadoop.hbase.test.MetricsAssertHelperImpl
import org.apache.hadoop.net.StaticMapping
import org.apache.hadoop.util.ToolRunner;

/* Runs IntegrationTestIngest on cluster
 *
 * Note: this test runs for about 20 minutes
 * please set hoya.test.timeout.seconds accordingly
 */
class TestHBaseIntegration extends TestFunctionalHBaseCluster {

  @Override
  String getClusterName() {
    return "test_hbase_integration"
  }

  @Override
  void clusterLoadOperations(
      Configuration clientConf,
      int numWorkers,
      Map<String, Integer> roleMap) {
    String parent = "/yarnapps_hoya_yarn_"+getClusterName()
    clientConf.set("zookeeper.znode.parent", parent)

    String[] args = []
    IntegrationTestIngest test = new IntegrationTestIngest();
    test.setConf(clientConf)
    int ret = ToolRunner.run(clientConf, test, args);
    assert ret == 0;
  }


  public int getWorkerPortAssignment() {
    return 0
  }

  public int getMasterPortAssignment() {
    return 0
  }
}