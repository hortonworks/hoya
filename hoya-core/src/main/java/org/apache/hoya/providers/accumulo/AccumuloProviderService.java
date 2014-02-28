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

import com.google.common.net.HostAndPort;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hoya.HoyaKeys;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.api.OptionKeys;
import org.apache.hoya.api.RoleKeys;
import org.apache.hoya.exceptions.BadClusterStateException;
import org.apache.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hoya.exceptions.BadConfigException;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.providers.AbstractProviderService;
import org.apache.hoya.providers.ProviderCore;
import org.apache.hoya.providers.ProviderRole;
import org.apache.hoya.providers.ProviderUtils;
import org.apache.hoya.servicemonitor.Probe;
import org.apache.hoya.tools.BlockingZKWatcher;
import org.apache.hoya.tools.ConfigHelper;
import org.apache.hoya.tools.HoyaFileSystem;
import org.apache.hoya.tools.HoyaUtils;
import org.apache.hoya.yarn.service.EventCallback;
import org.apache.hoya.yarn.service.EventNotifyingService;
import org.apache.hoya.yarn.service.ForkedProcessService;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hoya.HoyaKeys.*;
import static org.apache.hoya.providers.accumulo.AccumuloKeys.*;

/**
 * Server-side accumulo provider
 */
public class AccumuloProviderService extends AbstractProviderService implements
                                                                     ProviderCore {

  protected static final Logger log =
    LoggerFactory.getLogger(AccumuloClientProvider.class);
  private AccumuloClientProvider clientProvider;
  private static final ProviderUtils providerUtils = new ProviderUtils(log);
  
  private String masterAddress = null, monitorAddress = null;
  private HoyaFileSystem hoyaFileSystem = null;
  private ClusterDescription clusterSpec = null;
  private ZooCache zooCache = null;
  
  public AccumuloProviderService() {
    super("accumulo");
  }


  @Override
  public List<ProviderRole> getRoles() {
    return AccumuloRoles.ROLES;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    clientProvider = new AccumuloClientProvider(conf);
  }

  @Override
  public int getDefaultMasterInfoPort() {
    return 0;
  }

  @Override
  public void validateClusterSpec(ClusterDescription clusterSpec) throws
                                                                  HoyaException {
    clientProvider.validateClusterSpec(clusterSpec);
  }

  @Override
  public Configuration loadProviderConfigurationInformation(File confDir)
    throws BadCommandArgumentsException, IOException {

    return loadProviderConfigurationInformation(confDir, SITE_XML);
  }

  /*
   ======================================================================
   Server interface below here
   ======================================================================
  */
  @Override //server
  public void buildContainerLaunchContext(ContainerLaunchContext ctx,
                                          Container container,
                                          String role,
                                          HoyaFileSystem hoyaFileSystem,
                                          Path generatedConfPath,
                                          ClusterDescription clusterSpec,
                                          Map<String, String> roleOptions,
                                          Path containerTmpDirPath) throws
                                           IOException,
                                           BadConfigException {
    this.hoyaFileSystem = hoyaFileSystem;
    this.clusterSpec = clusterSpec;
    
    // Set the environment
    Map<String, String> env = HoyaUtils.buildEnvMap(roleOptions);
    env.put(ACCUMULO_LOG_DIR, ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    String hadoop_home =
      ApplicationConstants.Environment.HADOOP_COMMON_HOME.$();
    hadoop_home = clusterSpec.getOption(OPTION_HADOOP_HOME, hadoop_home);
    env.put(HADOOP_HOME, hadoop_home);
    env.put(HADOOP_PREFIX, hadoop_home);
    
    // By not setting ACCUMULO_HOME, this will cause the Accumulo script to
    // compute it on its own to an absolute path.

    env.put(ACCUMULO_CONF_DIR,
            ProviderUtils.convertToAppRelativePath(
              HoyaKeys.PROPAGATED_CONF_DIR_NAME));
    env.put(ZOOKEEPER_HOME, clusterSpec.getMandatoryOption(OPTION_ZK_HOME));

    ctx.setEnvironment(env);

    //local resources
    Map<String, LocalResource> localResources =
      new HashMap<String, LocalResource>();

    //add the configuration resources
    Map<String, LocalResource> confResources;
    confResources = hoyaFileSystem.submitDirectory(
            generatedConfPath,
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

    List<String> commands = new ArrayList<String>();
    List<String> command = new ArrayList<String>();
    
    String heap = "-Xmx" + clusterSpec.getRoleOpt(role, RoleKeys.JVM_HEAP, DEFAULT_JVM_HEAP);
    String opt = "ACCUMULO_OTHER_OPTS";
    if (HoyaUtils.isSet(heap)) {
      if (AccumuloKeys.ROLE_MASTER.equals(role)) {
        opt = "ACCUMULO_MASTER_OPTS";
      } else if (AccumuloKeys.ROLE_TABLET.equals(role)) {
        opt = "ACCUMULO_TSERVER_OPTS";
      } else if (AccumuloKeys.ROLE_MONITOR.equals(role)) {
        opt = "ACCUMULO_MONITOR_OPTS";
      } else if (AccumuloKeys.ROLE_GARBAGE_COLLECTOR.equals(role)) {
        opt = "ACCUMULO_GC_OPTS";
      }
      command.add(opt + "='" + heap + "'");
      env.put(opt, heap);
    }

    //this must stay relative if it is an image
    command.add(
      AccumuloClientProvider.buildScriptBinPath(clusterSpec).toString());

    //role is translated to the accumulo one
    command.add(AccumuloRoles.serviceForRole(role));
    
    // Add any role specific arguments to the command line
    String additionalArgs = ProviderUtils.getAdditionalArgs(roleOptions);
    if (!StringUtils.isBlank(additionalArgs)) {
      command.add(additionalArgs);
    }

    //log details
    command.add(
      "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/out.txt");
    command.add(
      "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/err.txt");

    String cmdStr = HoyaUtils.join(command, " ");

    commands.add(cmdStr);
    ctx.setCommands(commands);

  }
  
  public List<String> buildProcessCommandList(ClusterDescription clusterSpec,
                                          File confDir,
                                          Map<String, String> env,
                                          String... commands) throws
                                                                IOException,
                                                                HoyaException {
    env.put(ACCUMULO_LOG_DIR, ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    String hadoop_home = System.getenv(HADOOP_HOME);
    hadoop_home = clusterSpec.getOption(OPTION_HADOOP_HOME, hadoop_home);
    if (hadoop_home == null) {
      throw new BadConfigException(
        "Undefined env variable/config option: " + HADOOP_HOME);
    }
    ProviderUtils.validatePathReferencesLocalDir("HADOOP_HOME",hadoop_home);
    env.put(HADOOP_HOME, hadoop_home);
    env.put(HADOOP_PREFIX, hadoop_home);
    //buildup accumulo home env variable to be absolute or relative
    String accumulo_home = providerUtils.buildPathToHomeDir(
      clusterSpec, "bin", "accumulo");
    File image = new File(accumulo_home);
    String accumuloPath = image.getAbsolutePath();
    env.put(ACCUMULO_HOME, accumuloPath);
    ProviderUtils.validatePathReferencesLocalDir("ACCUMULO_HOME", accumuloPath);
    env.put(ACCUMULO_CONF_DIR, confDir.getAbsolutePath());
    String zkHome = clusterSpec.getMandatoryOption(OPTION_ZK_HOME);
    ProviderUtils.validatePathReferencesLocalDir("ZOOKEEPER_HOME", zkHome);

    env.put(ZOOKEEPER_HOME, zkHome);


    String accumuloScript = AccumuloClientProvider.buildScriptBinPath(clusterSpec);
    List<String> launchSequence = new ArrayList<String>(8);
    launchSequence.add(0, accumuloScript);
    Collections.addAll(launchSequence, commands);
    return launchSequence;
  }

  /**
   * Accumulo startup is a bit more complex than HBase, as it needs
   * to pre-initialize the data directory.
   *
   * This is done by running an init operation before starting the
   * real master. If the init fails, that is reported to the AM, which
   * then fails the application. 
   * If the init succeeds, the next service in the queue is started -
   * a composite service that starts the Accumulo Master and, in parallel,
   * sends a delayed event to the AM
   *
   * @param cd component description
   * @param confDir local dir with the config
   * @param env environment variables above those generated by
   * @param execInProgress callback for the event notification
   * @throws IOException IO problems
   * @throws HoyaException anything internal
   */
  @Override
  public boolean exec(ClusterDescription cd,
                      File confDir,
                      Map<String, String> env,
                      EventCallback execInProgress) throws
                                                 IOException,
                                                 HoyaException {


    //now pull in these files and do a bit of last-minute validation
    File siteXML = new File(confDir, SITE_XML);
    Configuration accumuloSite = ConfigHelper.loadConfFromFile(
      siteXML);
    String zkQuorum =
      accumuloSite.get(AccumuloConfigFileOptions.ZOOKEEPER_HOST);
    if (zkQuorum == null) {
      throw new BadConfigException("Accumulo site.xml %s does not contain %s",
                                   siteXML,
                                   AccumuloConfigFileOptions.ZOOKEEPER_HOST);
    } else {
      log.info("ZK Quorum is {}", zkQuorum);
    }
    //now test this
    int timeout = 5000;
    try {
      verifyZookeeperLive(zkQuorum, timeout);
      log.info("Zookeeper is live");
    } catch (KeeperException e) {
      throw new BadClusterStateException("Failed to connect to Zookeeper at %s after %d seconds",
                                         zkQuorum, timeout);
    } catch (InterruptedException e) {
      throw new BadClusterStateException(
        "Interrupted while trying to connect to Zookeeper at %s",
        zkQuorum);
    }
    boolean inited = isInited(cd);
    if (inited) {
      // cluster is inited, so don't run anything
      return false;
    }
    List<String> commands;

    log.info("Initializing accumulo datastore {}", cd.dataPath);
    commands = buildProcessCommandList(cd, confDir, env,
                            "init",
                            PARAM_INSTANCE_NAME,
                            providerUtils.getUserName() + "-" + cd.name,
                            PARAM_PASSWORD,
                            cd.getMandatoryOption(OPTION_ACCUMULO_PASSWORD),
                            "--clear-instance-name");


    ForkedProcessService accumulo =
      queueCommand(getName(), env, commands);
    //add a timeout to this process
    accumulo.setTimeout(
      cd.getOptionInt(OPTION_ACCUMULO_INIT_TIMEOUT,
                      INIT_TIMEOUT_DEFAULT), 1);
    
    //callback to AM to trigger cluster review is set up to happen after
    //the init/verify action has succeeded
    EventNotifyingService notifier = new EventNotifyingService(execInProgress,
      cd.getOptionInt( OptionKeys.CONTAINER_STARTUP_DELAY,
                       OptionKeys.DEFAULT_CONTAINER_STARTUP_DELAY));
    // register the service for lifecycle management; 
    // this service is started after the accumulo process completes
    addService(notifier);

    // now trigger the command sequence
    maybeStartCommandSequence();
    return true;
  }

  /**
   * probe to see if accumulo has already been installed.
   * @param cd cluster description
   * @return true if the relevant data directory looks inited
   * @throws IOException IO problems
   */
  private boolean isInited(ClusterDescription cd) throws IOException {
    Path accumuloInited = new Path(cd.dataPath, "instance_id");
    FileSystem fs2 = FileSystem.get(accumuloInited.toUri(), getConf());
    return fs2.exists(accumuloInited);
  }



  private void verifyZookeeperLive(String zkQuorum, int timeout) throws
                                                                 IOException,
                                                                 KeeperException,
                                                                 InterruptedException {

    BlockingZKWatcher watcher = new BlockingZKWatcher();
    ZooKeeper zookeeper = new ZooKeeper(zkQuorum, 10000, watcher, true);
    zookeeper.getChildren("/", watcher);

    watcher.waitForZKConnection(timeout);
    
    zooCache = ZooCache.getInstance(zkQuorum, 5 * 1000);
  }

  @Override
  public List<Probe> createProbes(ClusterDescription clusterSpec,
                                  String url,
                                  Configuration config,
                                  int timeout) throws IOException {
    return new ArrayList<Probe>(0);
  }
  
  @Override
  public Map<String, String> buildProviderStatus() {
    updateAccumuloInfo();
    
    Map<String,String> status = new HashMap<String, String>();
    
    status.put(AccumuloKeys.MASTER_ADDRESS, this.masterAddress);
    status.put(AccumuloKeys.MONITOR_ADDRESS, this.monitorAddress);
    
    return status;
  }
  

  @Override
  public boolean initMonitoring() {
    updateAccumuloInfo();
    return true;
  }
  
  private void updateAccumuloInfo() {
    if (null == hoyaFileSystem || null == clusterSpec || null == zooCache) {
      // Wait a while, the AM hasn't fully initialized things
      return;
    }
    
    String zkInstancePath;
    try {
      zkInstancePath = ZooUtil.getRoot(getInstanceId(hoyaFileSystem, clusterSpec));
    } catch (IOException e) {
      log.warn("Could not determine instanceID for Accumulo cluster {}", clusterSpec.name);
      return;
    }      
    
    byte[] masterData = ZooUtil.getLockData(zooCache, zkInstancePath + Constants.ZMASTER_LOCK);
    if (null != masterData) {
      this.masterAddress = new String(masterData, Constants.UTF8);
    }

    // TODO constant will exist in >=1.5.1
    byte[] monitorData = zooCache.get(zkInstancePath + "/monitor/http_addr");
    if (null != monitorData) {
      this.monitorAddress = new String(monitorData, Constants.UTF8);
    }
  }
  
  private String getInstanceId(HoyaFileSystem hoyaFs, ClusterDescription cd) throws IOException {
    // Should contain a single file whose name is the instance_id for this cluster
    FileStatus[] children = hoyaFs.getFileSystem().listStatus(new Path(cd.dataPath, "instance_id"));
    
    if (1 != children.length) {
      throw new IOException("Expected exactly one instance_id present, found " + children.length);
    }
    
    return children[0].getPath().getName();
  }
  
  /* non-javadoc
   * @see org.apache.hoya.providers.ProviderService#buildMonitorDetails()
   */
  @Override
  public TreeMap<String,URL> buildMonitorDetails(ClusterDescription clusterDesc) {
    TreeMap<String,URL> map = new TreeMap<String,URL>();
    
    map.put("Active Accumulo Master (RPC): " + getInfoAvoidingNull(clusterDesc, AccumuloKeys.MASTER_ADDRESS), null);
    
    String monitorKey = "Active Accumulo Monitor: ";
    String monitorAddr = getInfoAvoidingNull(clusterDesc, AccumuloKeys.MONITOR_ADDRESS);
    if (!StringUtils.isBlank(monitorAddr)) {
      try {
        HostAndPort hostPort = HostAndPort.fromString(monitorAddr);
        map.put(monitorKey, new URL("http", hostPort.getHostText(), hostPort.getPort(), ""));
      } catch (Exception e) {
        log.debug("Caught exception parsing Accumulo monitor URL", e);
        map.put(monitorKey + "N/A", null);
      }
    } else {
      map.put(monitorKey + "N/A", null);
    }

    return map;
  }
}
