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

package org.apache.hoya.providers.hoyaam;

import com.beust.jcommander.JCommander;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hoya.HoyaKeys;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.api.RoleKeys;
import org.apache.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hoya.exceptions.BadConfigException;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.exceptions.HoyaRuntimeException;
import org.apache.hoya.providers.AbstractProviderCore;
import org.apache.hoya.providers.ClientProvider;
import org.apache.hoya.providers.PlacementPolicy;
import org.apache.hoya.providers.ProviderRole;
import org.apache.hoya.providers.ProviderUtils;
import org.apache.hoya.tools.ConfigHelper;
import org.apache.hoya.tools.HoyaFileSystem;
import org.apache.hoya.tools.HoyaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hoya.HoyaKeys.*;

/**
 * handles the setup of the Hoya AM.
 * This keeps aspects of role, cluster validation and Clusterspec setup
 * out of the core hoya client
 */
public class HoyaAMClientProvider extends AbstractProviderCore implements
                                                               ClientProvider {


  protected static final Logger log =
    LoggerFactory.getLogger(HoyaAMClientProvider.class);
  protected static final String NAME = "hoyaAM";
  private static final ProviderUtils providerUtils = new ProviderUtils(log);
  public static final String AM_ROLE_CONFIG_RESOURCE =
    "org/apache/hoya/providers/hoyaam/role-am.xml";

  public HoyaAMClientProvider(Configuration conf) {
    super(conf);
  }

  /**
   * List of roles
   */
  protected static final List<ProviderRole> ROLES =
    new ArrayList<ProviderRole>();

  public static final int KEY_AM = ROLE_HOYA_AM_PRIORITY_INDEX;

  /**
   * Initialize role list
   */
  static {
    ROLES.add(new ProviderRole(ROLE_HOYA_AM, KEY_AM,
                               PlacementPolicy.EXCLUDE_FROM_FLEXING));
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public List<ProviderRole> getRoles() {
    return ROLES;
  }


  /**
   * Get a map of all the default options for the cluster; values
   * that can be overridden by user defaults after
   * @return a possibly empty map of default cluster options.
   */
  @Override
  public Configuration getDefaultClusterConfiguration() throws
                                                        FileNotFoundException {
    return ConfigHelper.loadMandatoryResource(
      "org/apache/hoya/providers/hoyaam/cluster.xml");
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
                                                                       FileNotFoundException {
    Map<String, String> rolemap = new HashMap<String, String>();
    if (rolename.equals(ROLE_HOYA_AM)) {
      Configuration conf = ConfigHelper.loadMandatoryResource(
        AM_ROLE_CONFIG_RESOURCE);
      HoyaUtils.mergeEntries(rolemap, conf);
    }
    return rolemap;
  }

  /**
   * This shouldn't be used, but is here as the API requires it.
   * @param clusterSpec this is the cluster specification used to define this
   * @return a map of the dynamic bindings for this Hoya instance
   */
  public Map<String, String> buildSiteConfFromSpec(ClusterDescription clusterSpec)
    throws BadConfigException {
    throw new HoyaRuntimeException("Not implemented");
  }

  /**
   * Build time review and update of the cluster specification
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

    //add a check for the directory being writeable by the current user
    String dataPath = clusterSpec.dataPath;
    Path path = new Path(dataPath);
    hoyaFileSystem.verifyDirectoryWriteAccess(path);
    Path historyPath = new Path(clusterDirPath, HoyaKeys.HISTORY_DIR_NAME);
    hoyaFileSystem.verifyDirectoryWriteAccess(historyPath);
  }

  @Override
  public void validateClusterSpec(
    ClusterDescription clusterSpec) throws
                                    HoyaException {
    Map<String, String> am = clusterSpec.getRole(ROLE_HOYA_AM);
    if (am == null) {
      throw new BadCommandArgumentsException("No Hoya Application master declared" 
                                             + " in cluster specification");
    }
    super.validateClusterSpec(clusterSpec);

    providerUtils.validateNodeCount(ROLE_HOYA_AM,
                                    clusterSpec.getDesiredInstanceCount(
                                      ROLE_HOYA_AM,
                                      0), 1, 1);

  }


  /**
   * The Hoya AM sets up all the dependency JARs above hoya.jar itself
   * {@inheritDoc}
   */
  @Override
  public Map<String, LocalResource> prepareAMAndConfigForLaunch(HoyaFileSystem hoyaFileSystem,
                                                                Configuration serviceConf,
                                                                ClusterDescription clusterSpec,
                                                                Path originConfDirPath,
                                                                Path generatedConfDirPath,
                                                                Configuration clientConfExtras,
                                                                String libdir,
                                                                Path tempPath)
    throws IOException, HoyaException {
    
    Map<String, LocalResource> providerResources =
      new HashMap<String, LocalResource>();
    HoyaUtils.putJar(providerResources,
                     hoyaFileSystem,
                     JCommander.class,
                     tempPath,
                     libdir,
                     JCOMMANDER_JAR);
    return providerResources;
  }

  /**
   * Update the AM resource with any local needs
   * @param capability capability to update
   */
  @Override
  public void prepareAMResourceRequirements(ClusterDescription clusterSpec,
                                            Resource capability) {
    capability.setMemory(clusterSpec.getRoleOptInt(
      HoyaKeys.ROLE_HOYA_AM,
      RoleKeys.YARN_MEMORY,
      capability.getMemory()));
    capability.setVirtualCores(clusterSpec.getRoleOptInt(
      HoyaKeys.ROLE_HOYA_AM, RoleKeys.YARN_CORES, capability.getVirtualCores()));
  }

  /**
   * Extract any JVM options from the cluster specification and
   * add them to the command line
   * @param clusterSpec spec
   */
  public void addJVMOptions(ClusterDescription clusterSpec,
                            List<String> commands) {
    commands.add(HoyaKeys.JVM_FORCE_IPV4);
    commands.add(HoyaKeys.JVM_JAVA_HEADLESS);
    String heap = clusterSpec.getRoleOpt(ROLE_HOYA_AM,
                                         RoleKeys.JVM_HEAP,
                                         DEFAULT_JVM_HEAP);
    if (HoyaUtils.isSet(heap)) {
      commands.add("-Xmx" + heap);
    }

    String jvmopts = clusterSpec.getRoleOpt(ROLE_HOYA_AM,
                                            RoleKeys.JVM_OPTS, "");
    if (HoyaUtils.isSet(jvmopts)) {
      commands.add(jvmopts);
    }

  }

  /**
   * Any operations to the service data before launching the AM
   * @param clusterSpec cspec
   * @param serviceData map of service data
   */
  @Override  //Client
  public void prepareAMServiceData(ClusterDescription clusterSpec,
                                   Map<String, ByteBuffer> serviceData) {

  }

  @Override
  public Configuration create(Configuration conf) {
    return conf;
  }
}
