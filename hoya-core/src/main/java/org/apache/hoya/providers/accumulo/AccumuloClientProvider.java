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

package org.apache.hoya.providers.accumulo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hoya.HoyaKeys;
import org.apache.hoya.HoyaXmlConfKeys;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.api.OptionKeys;
import org.apache.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hoya.exceptions.BadConfigException;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.providers.AbstractProviderCore;
import org.apache.hoya.providers.ClientProvider;
import org.apache.hoya.providers.ProviderRole;
import org.apache.hoya.providers.ProviderUtils;
import org.apache.hoya.tools.ConfigHelper;
import org.apache.hoya.tools.HoyaFileSystem;
import org.apache.hoya.tools.HoyaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.hoya.providers.accumulo.AccumuloConfigFileOptions.INSTANCE_SECRET;
import static org.apache.hoya.providers.accumulo.AccumuloKeys.*;

/**
 * Client-side accumulo provider
 */
public class AccumuloClientProvider extends AbstractProviderCore implements
                                                       ClientProvider {

  protected static final Logger log =
    LoggerFactory.getLogger(AccumuloClientProvider.class);
  private static final ProviderUtils providerUtils = new ProviderUtils(log);
  public static final String TEMPLATE_PATH =
    "org/apache/hoya/providers/accumulo/";

  protected AccumuloClientProvider(Configuration conf) {
    super(conf);
  }

  public static List<ProviderRole> getProviderRoles() {
    return AccumuloRoles.ROLES;
  }

  @Override
  public String getName() {
    return PROVIDER_ACCUMULO;
  }

  @Override
  public Configuration create(Configuration conf) {
    return conf;
  }

  @Override
  public List<ProviderRole> getRoles() {
    return AccumuloRoles.ROLES;
  }

  /**
   * Get a map of all the default options for the cluster; values
   * that can be overridden by user defaults after
   * @return a possibly emtpy map of default cluster options.
   */
  @Override
  public Configuration getDefaultClusterConfiguration() throws
                                                        FileNotFoundException {

    Configuration conf = ConfigHelper.loadMandatoryResource(
      "org/apache/hoya/providers/accumulo/accumulo.xml");

    //make up a password
    conf.set(OPTION_ACCUMULO_PASSWORD, UUID.randomUUID().toString());

    //create an instance ID
    conf.set(
      OptionKeys.SITE_XML_PREFIX + INSTANCE_SECRET,
      UUID.randomUUID().toString());
    return conf;

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
    if (rolename.equals(AccumuloKeys.ROLE_MASTER)) {
      // master role
      Configuration conf = ConfigHelper.loadMandatoryResource(
        TEMPLATE_PATH +"role-accumulo-master.xml");
      HoyaUtils.mergeEntries(rolemap, conf);
    } else if (rolename.equals(AccumuloKeys.ROLE_TABLET)) {
      // worker settings
      Configuration conf = ConfigHelper.loadMandatoryResource(
        TEMPLATE_PATH +"role-accumulo-tablet.xml");
      HoyaUtils.mergeEntries(rolemap, conf);
    } else if (rolename.equals(AccumuloKeys.ROLE_GARBAGE_COLLECTOR)) {
      Configuration conf = ConfigHelper.loadMandatoryResource(
        TEMPLATE_PATH +"role-accumulo-gc.xml");
      HoyaUtils.mergeEntries(rolemap, conf);
    } else if (rolename.equals(AccumuloKeys.ROLE_TRACER)) {
      Configuration conf = ConfigHelper.loadMandatoryResource(
        TEMPLATE_PATH +"role-accumulo-tracer.xml");
      HoyaUtils.mergeEntries(rolemap, conf);
    } else if (rolename.equals(AccumuloKeys.ROLE_MONITOR)) {
      Configuration conf = ConfigHelper.loadMandatoryResource(
        TEMPLATE_PATH +"role-accumulo-monitor.xml");
      HoyaUtils.mergeEntries(rolemap, conf);
    }
    return rolemap;
  }


  /**
   * Build the accumulo-site.xml file
   * @param clusterSpec this is the cluster specification used to define this
   * @return a map of the dynamic bindings for this Hoya instance
   */
  public Map<String, String> buildSiteConfFromSpec(ClusterDescription clusterSpec)
    throws BadConfigException {

    Map<String, String> sitexml = new HashMap<String, String>();


    providerUtils.propagateSiteOptions(clusterSpec, sitexml);

    String fsDefaultName =
      getConf().get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY);
    if (fsDefaultName == null) {
      throw new BadConfigException("Key not found in conf: {}",
                                   CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
    }
    sitexml.put(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, fsDefaultName);
    sitexml.put(HoyaXmlConfKeys.FS_DEFAULT_NAME_CLASSIC, fsDefaultName);

    String dataPath = clusterSpec.dataPath;
    Path path = new Path(dataPath);
    URI parentUri = path.toUri();
    String authority = parentUri.getAuthority();
    String fspath =
      parentUri.getScheme() + "://" + (authority == null ? "" : authority) + "/";
    sitexml.put(AccumuloConfigFileOptions.INSTANCE_DFS_URI, fspath);
    sitexml.put(AccumuloConfigFileOptions.INSTANCE_DFS_DIR,
                parentUri.getPath());

    //fix up ZK
    int zkPort = clusterSpec.getZkPort();
    String zkHosts = clusterSpec.getZkHosts();

    //parse the hosts
    String[] hostlist = zkHosts.split(",", 0);
    String quorum = HoyaUtils.join(hostlist, ":" + zkPort + ",");
    //this quorum has a trailing comma
    quorum = quorum.substring(0, quorum.length() - 1);
    sitexml.put(AccumuloConfigFileOptions.ZOOKEEPER_HOST, quorum);

    return sitexml;
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
    validateClusterSpec(clusterSpec);
  }

  /**
   * Add Accumulo and its dependencies (only) to the job configuration.
   * <p>
   * This is intended as a low-level API, facilitating code reuse between this
   * class and its mapred counterpart. It also of use to external tools that
   * need to build a MapReduce job that interacts with Accumulo but want
   * fine-grained control over the jars shipped to the cluster.
   * </p>
   *
   * @see org.apache.hadoop.hbase.mapred.TableMapReduceUtil
   * @see <a href="https://issues.apache.org/;jira/browse/PIG-3285">PIG-3285</a>
   *
   * @param providerResources provider resources to add resource to
   * @param hoyaFileSystem filesystem
   * @param libdir relative directory to place resources
   * @param tempPath path in the cluster FS for temp files
   * @throws IOException IO problems
   * @throws HoyaException Hoya-specific issues
   */
  public static void addAccumuloDependencyJars(Map<String, LocalResource> providerResources,
                                            HoyaFileSystem hoyaFileSystem,
                                            String libdir,
                                            Path tempPath) throws
                                                           IOException,
                                                           HoyaException {
    String[] jars =
      {
        "accumulo-core.jar",
        "accumulo-fate.jar",
        "zookeeper.jar",
      };
    Class<?>[] classes = {
      // accumulo-core
      org.apache.accumulo.core.Constants.class,
      // accumulo-fate
      org.apache.accumulo.fate.zookeeper.ZooLock.class,
      //zk
      org.apache.zookeeper.ClientCnxn.class
    };
    ProviderUtils.addDependencyJars(providerResources, hoyaFileSystem, tempPath,
                                    libdir, jars,
                                    classes);
  }

  /**
   * This builds up the site configuration for the AM and downstream services;
   * the path is added to the cluster spec so that launchers in the 
   * AM can pick it up themselves. 
   *
   *
   *
   *
   * @param hoyaFileSystem filesystem
   * @param serviceConf conf used by the service
   * @param clusterSpec cluster specification
   * @param originConfDirPath the original config dir -treat as read only
   * @param generatedConfDirPath path to place generated artifacts
   * @param clientConfExtras
   * @param libdir
   * @param tempPath
   * @return a map of name to local resource to add to the AM launcher
   */
  @Override //client
  public Map<String, LocalResource> prepareAMAndConfigForLaunch(HoyaFileSystem hoyaFileSystem,
                                                                Configuration serviceConf,
                                                                ClusterDescription clusterSpec,
                                                                Path originConfDirPath,
                                                                Path generatedConfDirPath,
                                                                Configuration clientConfExtras,
                                                                String libdir,
                                                                Path tempPath) throws
                                                                                           IOException,
                                                                                           HoyaException,
                                                                                           BadConfigException {
    Configuration siteConf = ConfigHelper.loadTemplateConfiguration(
      serviceConf,
      originConfDirPath,
      AccumuloKeys.SITE_XML,
      AccumuloKeys.SITE_XML_RESOURCE);

    //construct the cluster configuration values
    Map<String, String> clusterConfMap = buildSiteConfFromSpec(clusterSpec);
    //merge them
    ConfigHelper.addConfigMap(siteConf, clusterConfMap, "Accumulo Provider");

    if (log.isDebugEnabled()) {
      ConfigHelper.dumpConf(siteConf);
    }

    Path sitePath = ConfigHelper.saveConfig(serviceConf,
                                            siteConf,
                                            generatedConfDirPath,
                                            AccumuloKeys.SITE_XML);

    log.debug("Saving the config to {}", sitePath);
    Map<String, LocalResource> confResources;
    confResources = hoyaFileSystem.submitDirectory(generatedConfDirPath, HoyaKeys.PROPAGATED_CONF_DIR_NAME);

    addAccumuloDependencyJars(confResources, hoyaFileSystem, libdir, tempPath);
    
    return confResources;
  }

  /**
   * Update the AM resource with any local needs
   * @param capability capability to update
   */
  @Override //client
  public void prepareAMResourceRequirements(ClusterDescription clusterSpec,
                                            Resource capability) {
  }


  /**
   * Any operations to the service data before launching the AM
   * @param clusterSpec cspec
   * @param serviceData map of service data
   */
  @Override //client
  public void prepareAMServiceData(ClusterDescription clusterSpec,
                                   Map<String, ByteBuffer> serviceData) {

  }

  private static Set<String> knownRoleNames = new HashSet<String>();
  static {
    knownRoleNames.add(HoyaKeys.ROLE_HOYA_AM);
    for (ProviderRole role : AccumuloRoles.ROLES) {
      knownRoleNames.add(role.name);
    }
  }

  /**
   * Validate the cluster specification. This can be invoked on both
   * server and client
   * @param clusterSpec
   */
  @Override // Client and Server
  public void validateClusterSpec(ClusterDescription clusterSpec) throws
                                                                  HoyaException {
    super.validateClusterSpec(clusterSpec);
    Set<String> unknownRoles = clusterSpec.getRoleNames();
    unknownRoles.removeAll(knownRoleNames);
    if (!unknownRoles.isEmpty()) {
      throw new BadCommandArgumentsException("There is unknown role: %s",
        unknownRoles.iterator().next());
    }
    providerUtils.validateNodeCount(AccumuloKeys.ROLE_TABLET,
                                    clusterSpec.getDesiredInstanceCount(
                                      AccumuloKeys.ROLE_TABLET,
                                      1), 1, -1);


    providerUtils.validateNodeCount(AccumuloKeys.ROLE_MASTER,
                                    clusterSpec.getDesiredInstanceCount(
                                      AccumuloKeys.ROLE_MASTER,
                                      1), 1, -1);

    providerUtils.validateNodeCount(AccumuloKeys.ROLE_GARBAGE_COLLECTOR,
                                    clusterSpec.getDesiredInstanceCount(
                                      AccumuloKeys.ROLE_GARBAGE_COLLECTOR,
                                      0), 0, -1);

    providerUtils.validateNodeCount(AccumuloKeys.ROLE_MONITOR,
                                    clusterSpec.getDesiredInstanceCount(
                                      AccumuloKeys.ROLE_MONITOR,
                                      0), 0, -1);

    providerUtils.validateNodeCount(AccumuloKeys.ROLE_TRACER,
                                    clusterSpec.getDesiredInstanceCount(
                                      AccumuloKeys.ROLE_TRACER,
                                      0), 0, -1);

    clusterSpec.verifyOptionSet(AccumuloKeys.OPTION_ZK_HOME);
    clusterSpec.verifyOptionSet(AccumuloKeys.OPTION_HADOOP_HOME);
  }


  /**
   * Get the path to the script
   * @return the script
   */
  public static String buildScriptBinPath(ClusterDescription cd)
    throws FileNotFoundException {
    return providerUtils.buildPathToScript(
        cd, "bin", "accumulo");
  }


}
