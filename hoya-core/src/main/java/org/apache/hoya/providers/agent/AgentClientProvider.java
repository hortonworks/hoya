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
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hoya.HoyaKeys;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.api.RoleKeys;
import org.apache.hoya.exceptions.BadConfigException;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.providers.AbstractProviderCore;
import org.apache.hoya.providers.ClientProvider;
import org.apache.hoya.providers.ProviderRole;
import org.apache.hoya.providers.ProviderUtils;
import org.apache.hoya.tools.ConfigHelper;
import org.apache.hoya.tools.HoyaFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hoya.providers.agent.AgentKeys.*;

/**
 * This class implements  the client-side aspects
 * of the agent deployer
 */
public class AgentClientProvider extends AbstractProviderCore implements
                                                              ClientProvider {


  protected static final Logger log =
      LoggerFactory.getLogger(AgentClientProvider.class);
  protected static final String NAME = "agent";
  private static final ProviderUtils providerUtils = new ProviderUtils(log);


  protected AgentClientProvider(Configuration conf) {
    super(conf);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Configuration create(Configuration conf) {
    return conf;
  }

  @Override
  public List<ProviderRole> getRoles() {
    return AgentRoles.getRoles();
  }

  /**
   * Get a map of all the default options for the cluster; values
   * that can be overridden by user defaults after
   *
   * @return a possibly empty map of default cluster options.
   */
  @Override
  public Configuration getDefaultClusterConfiguration() throws
      FileNotFoundException {
    return ConfigHelper.loadMandatoryResource(
        "org/apache/hoya/providers/agent/agent.xml");
  }

  /**
   * Create the default cluster role instance for a named
   * cluster role;
   *
   * @param rolename role name
   * @return a node that can be added to the JSON
   */
  @Override
  public Map<String, String> createDefaultClusterRole(String rolename) throws
      HoyaException,
      IOException {
    Map<String, String> rolemap = new HashMap<String, String>();

    return rolemap;
  }

  /**
   * Build time review and update of the cluster specification
   *
   * @param clusterSpec spec
   */
  @Override // Client
  public void reviewAndUpdateClusterSpec(ClusterDescription clusterSpec) throws
      HoyaException {

    validateClusterSpec(clusterSpec);
  }

  @Override //Client
  public void preflightValidateClusterConfiguration(HoyaFileSystem hoyaFileSystem,
                                                    String clustername,
                                                    Configuration configuration,
                                                    ClusterDescription clusterSpec,
                                                    Path clusterDirPath,
                                                    Path generatedConfDirPath,
                                                    boolean secure) throws
      HoyaException,
      IOException {
    validateClusterSpec(clusterSpec);
  }


  /**
   * Validate the cluster specification. This can be invoked on both
   * server and client
   *
   * @param clusterSpec
   */
  @Override // Client and Server
  public void validateClusterSpec(ClusterDescription clusterSpec) throws
      HoyaException {
    log.debug(clusterSpec.toString());
    super.validateClusterSpec(clusterSpec);

    // Mandatory options for Agents
    // TODO: Enable these after CLI changes
    //clusterSpec.getMandatoryOption(CONTROLLER_URL);
    //clusterSpec.getMandatoryOption(PACKAGE_PATH);
    //clusterSpec.getMandatoryOption(AGENT_PATH);

    providerUtils.validateNodeCount(ROLE_NODE,
        clusterSpec.getDesiredInstanceCount(
            ROLE_NODE,
            1), 0, -1);
    Set<String> roleNames = clusterSpec.getRoleNames();
    roleNames.remove(HoyaKeys.ROLE_HOYA_AM);
    Map<Integer, String> priorityMap = new HashMap<Integer, String>();
    for (String roleName : roleNames) {
      int count =
          clusterSpec.getMandatoryRoleOptInt(roleName, RoleKeys.ROLE_INSTANCES);
      clusterSpec.getMandatoryRoleOpt(roleName, SCRIPT_PATH);
      // Extra validation for directly executed START
      if (!roleName.equals(ROLE_NODE)) {
        clusterSpec.getMandatoryRoleOpt(roleName, SERVICE_NAME);
        clusterSpec.getMandatoryRoleOpt(roleName, APP_HOME);
      }

      int priority =
          clusterSpec.getMandatoryRoleOptInt(roleName, RoleKeys.ROLE_PRIORITY);
      if (priority <= 0) {
        throw new BadConfigException("role %s %s value out of range %d",
            roleName,
            RoleKeys.ROLE_PRIORITY,
            priority);
      }

      String existing = priorityMap.get(priority);
      if (existing != null) {
        throw new BadConfigException(
            "role %s has a %s value %d which duplicates that of %s",
            roleName,
            RoleKeys.ROLE_PRIORITY,
            priority,
            existing);
      }
      priorityMap.put(priority, roleName);
    }
  }

  @Override
  public Map<String, LocalResource> prepareAMAndConfigForLaunch(HoyaFileSystem hoyaFileSystem,
                                                                Configuration serviceConf,
                                                                ClusterDescription clusterSpec,
                                                                Path originConfDirPath,
                                                                Path generatedConfDirPath,
                                                                Configuration clientConfExtras,
                                                                String libdir,
                                                                Path tempPath) throws
      IOException,
      HoyaException {
    //load in the template site config
    log.debug("Loading template configuration from {}, saving to ",
              originConfDirPath, generatedConfDirPath);

    Path commandJson =
      new Path(originConfDirPath, AgentKeys.COMMAND_JSON_FILENAME);
    hoyaFileSystem.verifyFileExists(commandJson);
    

    Map<String, LocalResource> providerResources;
    providerResources = hoyaFileSystem.submitDirectory(generatedConfDirPath,
        HoyaKeys.PROPAGATED_CONF_DIR_NAME);

    return providerResources;
  }

  /**
   * Update the AM resource with any local needs
   *
   * @param capability capability to update
   */
  @Override
  public void prepareAMResourceRequirements(ClusterDescription clusterSpec,
                                            Resource capability) {

  }

  /**
   * Any operations to the service data before launching the AM
   *
   * @param clusterSpec cspec
   * @param serviceData map of service data
   */
  @Override  //Client
  public void prepareAMServiceData(ClusterDescription clusterSpec,
                                   Map<String, ByteBuffer> serviceData) {

  }


}
