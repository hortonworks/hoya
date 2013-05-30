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

package org.apache.hadoop.hoya.yarn.client

import com.beust.jcommander.Parameter
import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hoya.tools.PathArgumentConverter
import org.apache.hadoop.hoya.tools.URLArgumentConverter
import org.apache.hadoop.hoya.yarn.CommonArgs
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * Args list for JCommanderification
 */
@Commons
class ClientArgs extends CommonArgs {

  /**
   * Name of entry class: {@value}
   */
  public static final String CLASSNAME = "org.apache.hadoop.hoya.yarn.client.HoyaClient"
  /**
   filesystem-uri: {@value}
   */
  public static final String ARG_AMQUEUE = "--amqueue"
  public static final String ARG_AMPRIORITY = "--ampriority"
  public static final String ARG_CONFDIR = "--confdir"
  public static final String ARG_FILESYSTEM = "--fs"
  public static final String ARG_FORMAT = "--format"
  public static final String ARG_IMAGE = "--image"
  public static final String ARG_MANAGER = "--manager"
  public static final String ARG_WAIT = "--wait"


  @Parameter(names = "--amqueue", description = "Application Manager Queue Name")
  String amqueue = "default";

  @Parameter(names = "--ampriority", description = "YARN Scheduling priority for the Application Manager")
  int ampriority = 0;


  @Parameter(names = "--fs", description = "filesystem URI",
      converter = URLArgumentConverter)
  URL filesystemURL;

  //--format 
  @Parameter(names = "--format", description = "format for a response text|xml|json|properties")
  String format;

  //--wait [timeout]
  @Parameter(names = "--wait",
      description = "time to wait for an action to complete")
  int waittime = 0

  /**
   * --image path
   the full path to a .tar or .tar.gz path containing an HBase image.
   */
  @Parameter(names = "--image",
      description = "the full path to a .tar or .tar.gz path containing an HBase image",
      converter = PathArgumentConverter)
  Path image

  /**
   *    Declare the image configuration directory to use when creating or reconfiguring a hoya cluster. The path must be on a filesystem visible to all nodes in the YARN cluster.
   Only one configuration directory can be specified.
   */
  @Parameter(names = "--confdir",
      description = "path cluster configuration directory",
      converter = PathArgumentConverter)
  Path confdir

  @Parameter(names = ["--m","--manager"],
      description = "hostname:port of the YARN resource manager")
  String manager;

/*
  @Parameter(names = "--", description = "")
  @Parameter(names = "--", description = "")
*/

  /**

   -m, --manager url
   URL of the YARN resource manager
   This could just be kept in ZK at a well-known place, though that implies an init process to set it.
*/


  /**
   * map of actions -> (explanation, min #of entries [, max no.])
   * If the max no is not given it is assumed to be the same as the min no.
   */
  static final Map<String,List> ACTIONS = [
      (ACTION_CREATE): ["create cluster", 1],
      (ACTION_PREFLIGHT): ["Perform preflight checks", 0],
      (ACTION_LIST): ["List running cluster", 0],
      (ACTION_STATUS): ["Get the status of a cluster", 1],
      (ACTION_STOP): ["stop a cluster", 1],
      (ACTION_START): ["start a cluster", 1],
      (ACTION_ISLIVE): ["probe for a cluster being live", 1],
      (ACTION_ADDNODE): ["add nodes", 1],
      (ACTION_GETSIZE): ["get the size of a cluster", 1],
      (ACTION_RMNODE): ["remove nodes", 1],
      (ACTION_RECONFIGURE): ["change the configuration of a cluser", 1],
      (ACTION_REIMAGE): ["change the image a cluster uses", 1],
      (ACTION_MIGRATE): ["migrate cluster to a new HBase version", 1],
      (ACTION_HELP): ["Print Help information", 0],
  ]

  ClientArgs(String[] args) {
    super(args)
  }

  @Override
  Map<String, List> getActions() {
    return ACTIONS
  }
  
  @Override
  void validate() {
    super.validate()
  }

  @Override
  void postProcess() {
    super.postProcess()
  }

  @Override
  void applyDefinitions(Configuration conf) {
    super.applyDefinitions(conf)
    //RM
    if (manager) {
      log.debug("Setting RM to $manager")
      conf.set(YarnConfiguration.RM_ADDRESS, manager)
    }
  }
}