/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hoya.providers.accumulo;

/**
 * Mappings of config params to env variables for
 * custom -site.xml files to pick up
 *
 * A lot of these come from HConstants -the reason they have been copied
 * and pasted in here is to remove dependencies on HBase from
 * the Hoya Client and AM.
 */
public interface AccumuloConfigFileOptions {


  /**
   * quorum style, comma separated list of hostname:port values
   */
  String ZOOKEEPER_HOST = "instance.zookeeper.host";

  /**
   * URI to the filesystem
   */
  String INSTANCE_DFS_URI = "instance.dfs.uri";

  /**
   * Dir under the DFS URI
   */
  String INSTANCE_DFS_DIR = "instance.dfs.dir";


  String INSTANCE_SECRET = "instance.secret";
  String MASTER_PORT_CLIENT = "master.port.client";
  String MASTER_PORT_CLIENT_DEFAULT = "9999";
  
  String MONITOR_PORT_CLIENT = "monitor.port.client";
  int MONITOR_PORT_CLIENT_INT = 50095;
  String MONITOR_PORT_CLIENT_DEFAULT = ""+MONITOR_PORT_CLIENT_INT;
  String TRACE_PORT_CLIENT = "trace.port.client";
  String TRACE_PORT_CLIENT_DEFAULT = "12234";

  String TSERV_PORT_CLIENT = "tserver.port.client";
  String TSERV_PORT_CLIENT_DEFAULT = "9997";
  
}
