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

package org.apache.hoya.providers.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hoya.HoyaKeys;
import org.apache.hoya.HoyaXmlConfKeys;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.api.OptionKeys;
import org.apache.hoya.api.RoleKeys;
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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hoya.providers.hbase.HBaseConfigFileOptions.*;

/**
 * This class implements  the client-side aspects
 * of an HBase Cluster
 */
public class HBaseClientProvider extends AbstractProviderCore implements
                                                              ClientProvider {



  
  protected static final Logger log =
    LoggerFactory.getLogger(HBaseClientProvider.class);
  protected static final String NAME = "hbase";
  private static final ProviderUtils providerUtils = new ProviderUtils(log);


  protected HBaseClientProvider(Configuration conf) {
    super(conf);
  }

  @Override
  public String getName() {
    return NAME;
  }



  @Override
  public Configuration create(Configuration conf) {
    return HBaseConfiguration.create(conf);
  }



  @Override
  public List<ProviderRole> getRoles() {
    return HBaseRoles.getRoles();
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
      "org/apache/hoya/providers/hbase/hbase.xml");
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
                                                                       HoyaException, IOException {
    Map<String, String> rolemap = new HashMap<String, String>();
    if (rolename.equals(HBaseKeys.ROLE_MASTER)) {
      // master role
      Configuration conf = ConfigHelper.loadMandatoryResource(
        "org/apache/hoya/providers/hbase/role-hbase-master.xml");
      HoyaUtils.mergeEntries(rolemap, conf);
    } else if (rolename.equals(HBaseKeys.ROLE_WORKER)) {
      // worker settings
      Configuration conf = ConfigHelper.loadMandatoryResource(
        "org/apache/hoya/providers/hbase/role-hbase-worker.xml");
      HoyaUtils.mergeEntries(rolemap, conf);
    }
    return rolemap;
  }

  /**
   * Build the hdfs-site.xml file
   * This the configuration used by HBase directly
   * @param clusterSpec this is the cluster specification used to define this
   * @return a map of the dynamic bindings for this Hoya instance
   */
  public Map<String, String> buildSiteConfFromSpec(ClusterDescription clusterSpec)
    throws BadConfigException {

    Map<String, String> master = clusterSpec.getMandatoryRole(
      HBaseKeys.ROLE_MASTER);

    Map<String, String> worker = clusterSpec.getMandatoryRole(
      HBaseKeys.ROLE_WORKER);

    Map<String, String> sitexml = new HashMap<String, String>();
    
    //map all cluster-wide site. options
    providerUtils.propagateSiteOptions(clusterSpec, sitexml);
/*
  //this is where we'd do app-indepdenent keytabs

    String keytab =
      clusterSpec.getOption(OptionKeys.OPTION_KEYTAB_LOCATION, "");
    
*/


    sitexml.put(KEY_HBASE_CLUSTER_DISTRIBUTED, "true");
    sitexml.put(KEY_HBASE_MASTER_PORT, "0");

    sitexml.put(KEY_HBASE_MASTER_INFO_PORT, master.get(
      RoleKeys.APP_INFOPORT));
    sitexml.put(KEY_HBASE_ROOTDIR,
                clusterSpec.dataPath);
    sitexml.put(KEY_REGIONSERVER_INFO_PORT,
                worker.get(RoleKeys.APP_INFOPORT));
    sitexml.put(KEY_REGIONSERVER_PORT, "0");
    providerUtils.propagateOption(clusterSpec, OptionKeys.ZOOKEEPER_PATH,
                                  sitexml, KEY_ZNODE_PARENT);
    providerUtils.propagateOption(clusterSpec, OptionKeys.ZOOKEEPER_PORT,
                                  sitexml, KEY_ZOOKEEPER_PORT);
    providerUtils.propagateOption(clusterSpec, OptionKeys.ZOOKEEPER_HOSTS,
                                  sitexml, KEY_ZOOKEEPER_QUORUM);

    return sitexml;
  }

  /**
   * Build time review and update of the cluster specification
   * @param clusterSpec spec
   */
  @Override // Client
  public void reviewAndUpdateClusterSpec(ClusterDescription clusterSpec) throws
                                                                         HoyaException{

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
    Path templatePath = new Path(generatedConfDirPath, HBaseKeys.SITE_XML);
    //load the HBase site file or fail
    Configuration siteConf = ConfigHelper.loadConfiguration(hoyaFileSystem.getFileSystem(),
                                                            templatePath);

    //core customizations
    validateHBaseSiteXML(siteConf, secure, templatePath.toString());

  }

  /**
   * Validate the hbase-site.xml values
   * @param siteConf site config
   * @param secure secure flag
   * @param origin origin for exceptions
   * @throws BadConfigException if a config is missing/invalid
   */
  public void validateHBaseSiteXML(Configuration siteConf,
                                    boolean secure,
                                    String origin) throws BadConfigException {
    try {
      HoyaUtils.verifyOptionSet(siteConf, KEY_HBASE_CLUSTER_DISTRIBUTED,
                                false);
      HoyaUtils.verifyOptionSet(siteConf, KEY_HBASE_ROOTDIR, false);
      HoyaUtils.verifyOptionSet(siteConf, KEY_ZNODE_PARENT, false);
      HoyaUtils.verifyOptionSet(siteConf, KEY_ZOOKEEPER_QUORUM, false);
      HoyaUtils.verifyOptionSet(siteConf, KEY_ZOOKEEPER_PORT, false);
      int zkPort =
        siteConf.getInt(HBaseConfigFileOptions.KEY_ZOOKEEPER_PORT, 0);
      if (zkPort == 0) {
        throw new BadConfigException(
          "ZK port property not provided at %s in configuration file %s",
          HBaseConfigFileOptions.KEY_ZOOKEEPER_PORT,
          siteConf);
      }

      if (secure) {
        //better have the secure cluster definition up and running
        HoyaUtils.verifyOptionSet(siteConf, KEY_MASTER_KERBEROS_PRINCIPAL,
                                  false);
        HoyaUtils.verifyOptionSet(siteConf, KEY_MASTER_KERBEROS_KEYTAB,
                                  false);
        HoyaUtils.verifyOptionSet(siteConf,
                                  KEY_REGIONSERVER_KERBEROS_PRINCIPAL,
                                  false);
        HoyaUtils.verifyOptionSet(siteConf,
                                  KEY_REGIONSERVER_KERBEROS_KEYTAB, false);
      }
    } catch (BadConfigException e) {
      //bad configuration, dump it

      log.error("Bad site configuration {} : {}", origin, e, e);
      log.info(ConfigHelper.dumpConfigToString(siteConf));
      throw e;
    }
  }

  private static Set<String> knownRoleNames = new HashSet<String>();
  static {
    List<ProviderRole> roles = HBaseRoles.getRoles();
    knownRoleNames.add(HoyaKeys.ROLE_HOYA_AM);
    for (ProviderRole role : roles) {
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
    providerUtils.validateNodeCount(HBaseKeys.ROLE_WORKER,
                                    clusterSpec.getDesiredInstanceCount(
                                      HBaseKeys.ROLE_WORKER,
                                      0), 0, -1);


    providerUtils.validateNodeCount(HBaseKeys.ROLE_MASTER,
                                    clusterSpec.getDesiredInstanceCount(
                                      HBaseKeys.ROLE_MASTER,
                                      0),
                                    0,
                                    -1);
  }

  /**
   * Add HBase and its dependencies (only) to the job configuration.
   * <p>
   * This is intended as a low-level API, facilitating code reuse between this
   * class and its mapred counterpart. It also of use to extenral tools that
   * need to build a MapReduce job that interacts with HBase but want
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
  public static void addHBaseDependencyJars(Map<String, LocalResource> providerResources,
                                            HoyaFileSystem hoyaFileSystem,
                                            String libdir,
                                            Path tempPath) throws
                                                           IOException,
                                                           HoyaException {
    String[] jars =
      {
        "hbase-common.jar",
        "hbase-protocol.jar",
        "hbase-client.jar",
        "zookeeper.jar",
        "htrace-core.jar",
      };
    Class<?>[] classes = {
      // hbase-common
      org.apache.hadoop.hbase.HConstants.class,
      // hbase-protocol
      org.apache.hadoop.hbase.protobuf.generated.ClientProtos.class,
      // hbase-client
      org.apache.hadoop.hbase.client.Put.class,
      //zk
      org.apache.zookeeper.ClientCnxn.class,
      // HTrace
      org.cloudera.htrace.Trace.class
    };
    ProviderUtils.addDependencyJars(providerResources, hoyaFileSystem, tempPath,
                                    libdir, jars,
                                    classes);
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
    log.debug("Loading template configuration from {}", originConfDirPath);
    Configuration siteConf = ConfigHelper.loadTemplateConfiguration(
      serviceConf,
      originConfDirPath,
      HBaseKeys.SITE_XML,
      HBaseKeys.HBASE_TEMPLATE_RESOURCE);
    
    if (log.isDebugEnabled()) {
      log.debug("Configuration came from {}",
                siteConf.get(HoyaXmlConfKeys.KEY_HOYA_TEMPLATE_ORIGIN));
      ConfigHelper.dumpConf(siteConf);
    }
    //construct the cluster configuration values
    Map<String, String> clusterConfMap = buildSiteConfFromSpec(clusterSpec);
    
    //merge them
    ConfigHelper.addConfigMap(siteConf,
                              clusterConfMap.entrySet(),
                              "HBase Provider");

    //now, if there is an extra client conf, merge it in too
    if (clientConfExtras != null) {
      ConfigHelper.mergeConfigurations(siteConf, clientConfExtras,
                                       "Hoya Client");
    }
    
    if (log.isDebugEnabled()) {
      log.debug("Merged Configuration");
      ConfigHelper.dumpConf(siteConf);
    }

    Path sitePath = ConfigHelper.saveConfig(serviceConf,
                                            siteConf,
                                            generatedConfDirPath,
                                            HBaseKeys.SITE_XML);

    log.debug("Saving the config to {}", sitePath);
    Map<String, LocalResource> providerResources;
    providerResources = hoyaFileSystem.submitDirectory(generatedConfDirPath,
                                       HoyaKeys.PROPAGATED_CONF_DIR_NAME);

    addHBaseDependencyJars(providerResources, hoyaFileSystem,libdir, tempPath);

    return providerResources;
  }

  /**
   * Update the AM resource with any local needs
   * @param capability capability to update
   */
  @Override
  public void prepareAMResourceRequirements(ClusterDescription clusterSpec,
                                            Resource capability) {

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



}
