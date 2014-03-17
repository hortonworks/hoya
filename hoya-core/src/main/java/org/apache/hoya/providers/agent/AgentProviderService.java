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

package org.apache.hoya.providers.agent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hoya.HoyaKeys;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.providers.AbstractProviderService;
import org.apache.hoya.providers.ProviderCore;
import org.apache.hoya.providers.ProviderRole;
import org.apache.hoya.providers.ProviderUtils;
import org.apache.hoya.servicemonitor.Probe;
import org.apache.hoya.tools.HoyaFileSystem;
import org.apache.hoya.tools.HoyaUtils;
import org.apache.hoya.yarn.service.EventCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hoya.providers.agent.AgentKeys.*;

/**
 * This class implements the server-side aspects
 * of an agent deployment
 */
public class AgentProviderService extends AbstractProviderService implements
                                                                  ProviderCore {


  protected static final Logger log =
      LoggerFactory.getLogger(AgentProviderService.class);
  protected static final String NAME = "agent";
  private static final ProviderUtils providerUtils = new ProviderUtils(log);
  private AgentClientProvider clientProvider;

  public AgentProviderService() {
    super("AgentProviderService");
  }

  @Override
  public List<ProviderRole> getRoles() {
    return AgentRoles.getRoles();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    clientProvider = new AgentClientProvider(conf);
  }

  @Override
  public int getDefaultMasterInfoPort() {
    return 0;
  }

  @Override
  public Configuration loadProviderConfigurationInformation(File confDir) throws
                                                                          BadCommandArgumentsException,
                                                                          IOException {
    return new Configuration(false);
  }

  @Override
  public void validateClusterSpec(ClusterDescription clusterSpec) throws
                                                                  HoyaException {
    clientProvider.validateClusterSpec(clusterSpec);
  }

  @Override  // server
  public void buildContainerLaunchContext(ContainerLaunchContext ctx,
                                          Container container,
                                          String role,
                                          HoyaFileSystem hoyaFileSystem,
                                          Path generatedConfPath,
                                          ClusterDescription clusterSpec,
                                          Map<String, String> roleOptions,
                                          Path containerTmpDirPath) throws
                                                                    IOException,
                                                                    HoyaException {
    log.info("Build launch context for Agent");
    log.debug(clusterSpec.toString());

    // Set the environment
    Map<String, String> env = HoyaUtils.buildEnvMap(roleOptions);

    HoyaUtils.copyDirectory(getConf(), generatedConfPath, containerTmpDirPath,
                            null);
    Path targetConfDir = containerTmpDirPath;
    //TODO: PATCH THE CONFIG FOR THE TARGET


    String propagatedConfDir = ApplicationConstants.Environment.PWD.$() + "/" +
        HoyaKeys.PROPAGATED_CONF_DIR_NAME;
    env.put("PROPAGATED_CONFDIR", propagatedConfDir);
    //local resources
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();

    //add the configuration resources
    Map<String, LocalResource> confResources;
    confResources = hoyaFileSystem.submitDirectory(
        targetConfDir,
        HoyaKeys.PROPAGATED_CONF_DIR_NAME);
    localResources.putAll(confResources);
    //Add binaries
    //now add the image if it was set
    if (clusterSpec.isImagePathSet()) {
      Path imagePath = new Path(clusterSpec.getImagePath());
      log.info("using image path {}", imagePath);
      hoyaFileSystem.maybeAddImagePath(localResources, imagePath);
    }
    ctx.setLocalResources(localResources);
    List<String> commandList = new ArrayList<String>();

    List<String> operation = new ArrayList<String>();


    String script =
        clusterSpec.getMandatoryRoleOpt(role, SCRIPT_PATH);
    String packagePath = clusterSpec.getMandatoryRoleOpt(role, PACKAGE_PATH);
    File packagePathFile = new File(packagePath);
    HoyaUtils.verifyIsDir(packagePathFile, log);
    File executable = new File(packagePathFile, script);
    HoyaUtils.verifyFileExists(executable, log);

    String appHome =
        clusterSpec.getMandatoryRoleOpt(role, APP_HOME);
    //APP_HOME == /dev/null is being used to issue direct start commands
    //This is not required once embedded Agent is available
    if (appHome.equals("/dev/null")) {
      operation.add("python");
      operation.add(executable.getCanonicalPath());
      operation.add("START");
      operation.add(propagatedConfDir + "/" + AgentKeys.COMMAND_JSON_FILENAME);
      operation.add(packagePathFile.getCanonicalPath());
      operation.add("/tmp/strout.txt");
    } else {
      //this must stay relative if it is an image
      operation.add("python");
      operation.add(executable.getCanonicalPath());
      operation.add(ARG_LOG);
      operation.add(ApplicationConstants.LOG_DIR_EXPANSION_VAR);
      operation.add(ARG_COMMAND);
      operation.add(propagatedConfDir + "/" + AgentKeys.COMMAND_JSON_FILENAME);

      operation.add(ARG_CONFIG);
      operation.add("$PROPAGATED_CONFDIR");
    }

    String filename = "agent-server.txt";

    operation.add(
        "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + filename);
    operation.add("2>&1");

    String cmdStr = HoyaUtils.join(operation, " ");
    commandList.add(cmdStr);
    ctx.setCommands(commandList);
    ctx.setEnvironment(env);
}

  public void appendOperation(List<String> commandList,
                              String exe,
                              String filename) {
    List<String> operation = new ArrayList<String>();
    operation.add(exe);
    operation.add(
        "1>>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + filename);
    operation.add("2>&1");
    String cmdStr = HoyaUtils.join(operation, " ");
    commandList.add(cmdStr);
  }

  /**
   * Run this service
   *
   * @param cd             component description
   * @param confDir        local dir with the config
   * @param env            environment variables above those generated by
   * @param execInProgress callback for the event notification
   * @throws IOException   IO problems
   * @throws HoyaException anything internal
   */
  @Override
  public boolean exec(ClusterDescription cd,
                      File confDir,
                      Map<String, String> env,
                      EventCallback execInProgress) throws
                                                    IOException,
                                                    HoyaException {

    return false;
  }

  /**
   * This is a validation of the application configuration on the AM.
   * Here is where things like the existence of keytabs and other
   * not-seen-client-side properties can be tested, before
   * the actual process is spawned.
   *
   * @param clusterSpec clusterSpecification
   * @param confDir     configuration directory
   * @param secure      flag to indicate that secure mode checks must exist
   * @throws IOException   IO problemsn
   * @throws HoyaException any failure
   */
  @Override
  public void validateApplicationConfiguration(ClusterDescription clusterSpec,
                                               File confDir,
                                               boolean secure
  ) throws IOException, HoyaException {

  }

  @Override
  public boolean initMonitoring() {
    return true;
  }

  @Override
  public List<Probe> createProbes(ClusterDescription clusterSpec, String urlStr,
                                  Configuration config,
                                  int timeout)
      throws IOException {
    List<Probe> probes = new ArrayList<Probe>();

    return probes;
  }

  /**
   * Build the provider status, can be empty
   *
   * @return the provider status - map of entries to add to the info section
   */
  public Map<String, String> buildProviderStatus() {
    Map<String, String> stats = new HashMap<String, String>();
    return stats;
  }

  @Override
  public boolean isSupportedRole(String role) {
    return true;
  }
}
