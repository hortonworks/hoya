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

package org.apache.hoya.providers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.service.launcher.ExitCodeProvider;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.tools.ConfigHelper;
import org.apache.hoya.tools.HoyaUtils;
import org.apache.hoya.yarn.service.ForkedProcessService;
import org.apache.hoya.yarn.service.Parent;
import org.apache.hoya.yarn.service.SequenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The base class for provider services. It lets the implementations
 * add sequences of operations, and propagates service failures
 * upstream
 */
public abstract class AbstractProviderService
                          extends SequenceService
                          implements
                            ProviderCore,
                            ProviderService {
  private static final Logger log =
    LoggerFactory.getLogger(AbstractProviderService.class);
  
  public AbstractProviderService(String name) {
    super(name);
  }

  /**
   * Build a string command from a list of objects
   * @param args arguments
   * @return a space separated string of all the arguments' string values
   */
  protected String cmd(Object... args) {
    List<String> list = new ArrayList<String>(args.length);
    for (Object arg : args) {
      list.add(arg.toString());
    }
    return HoyaUtils.join(list, " ");
  }

  @Override
  public Configuration getConf() {
    return getConfig();
  }

  /**
   * Load a specific XML configuration file for the provider config
   * @param confDir configuration directory
   * @param siteXMLFilename provider-specific filename
   * @return a configuration to be included in status
   * @throws BadCommandArgumentsException argument problems
   * @throws IOException IO problems
   */
  protected Configuration loadProviderConfigurationInformation(File confDir,
                                                               String siteXMLFilename)
    throws BadCommandArgumentsException, IOException {
    Configuration siteConf;
    File siteXML = new File(confDir, siteXMLFilename);
    if (!siteXML.exists()) {
      throw new BadCommandArgumentsException(
        "Configuration directory %s doesn't contain %s - listing is %s",
        confDir, siteXMLFilename, HoyaUtils.listDir(confDir));
    }

    //now read it in
    siteConf = ConfigHelper.loadConfFromFile(siteXML);
    log.info("{} file is at {}", siteXMLFilename, siteXML);
    log.info(ConfigHelper.dumpConfigToString(siteConf));
    return siteConf;
  }

  /**
   * No-op implementation of this method.
   * 
   * {@inheritDoc}
   */
  @Override
  public void validateApplicationConfiguration(ClusterDescription clusterSpec,
                                               File confDir,
                                               boolean secure) throws
                                                               IOException,
                                                               HoyaException {
    
  }

  /**
   * Scan through the roles and see if it is supported.
   * @param role role to look for
   * @return true if the role is known about -and therefore
   * that a launcher thread can be deployed to launch it
   */
  @Override
  public boolean isSupportedRole(String role) {
    Collection<ProviderRole> roles = getRoles();
    for (ProviderRole providedRole : roles) {
      if (providedRole.name.equals(role)) {
        return true;
      }
    }
    return false;
  }
  
  @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
  @Override // ExitCodeProvider
  public int getExitCode() {
    Throwable cause = getFailureCause();
    if (cause != null) {
      //failed for some reason
      if (cause instanceof ExitCodeProvider) {
        return ((ExitCodeProvider) cause).getExitCode();
      }
    }
    ForkedProcessService lastProc = latestProcess();
    if (lastProc == null) {
      return 0;
    } else {
      return lastProc.getExitCode();
    }
  }

  /**
   * Return the latest forked process service that ran
   * @return the forkes service
   */
  protected ForkedProcessService latestProcess() {
    Service current = getCurrentService();
    Service prev = getPreviousService();

    Service latest = current != null ? current : prev;
    if (latest instanceof ForkedProcessService) {
      return (ForkedProcessService) latest;
    } else {
      //its a composite object, so look inside it for a process
      if (latest instanceof Parent) {
        return getFPSFromParentService((Parent) latest);
      } else {
        //no match
        return null;
      }
    }
  }


  /**
   * Given a parent service, find the one that is a forked process
   * @param parent parent
   * @return the forked process service or null if there is none
   */
  protected ForkedProcessService getFPSFromParentService(Parent parent) {
    List<Service> services = parent.getServices();
    for (Service s : services) {
      if (s instanceof ForkedProcessService) {
        return (ForkedProcessService) s;
      }
    }
    return null;
  }

  /**
   * if we are already running, start this service
   */
  protected void maybeStartCommandSequence() {
    if (isInState(STATE.STARTED)) {
      startNextService();
    }
  }

  /**
   * Create a new forked process service with the given
   * name, environment and command list -then add it as a child
   * for execution in the sequence.
   *
   * @param name command name
   * @param env environment
   * @param commands command line
   * @throws IOException
   * @throws HoyaException
   */
  protected ForkedProcessService queueCommand(String name,
                              Map<String, String> env,
                              List<String> commands) throws
                                                     IOException,
                                                     HoyaException {
    ForkedProcessService process = buildProcess(name, env, commands);
    //register the service for lifecycle management; when this service
    //is terminated, so is the master process
    addService(process);
    return process;
  }

  public ForkedProcessService buildProcess(String name,
                                           Map<String, String> env,
                                           List<String> commands) throws
                                                                  IOException,
                                                                  HoyaException {
    ForkedProcessService process;
    process = new ForkedProcessService(name);
    process.init(getConfig());
    process.build(env, commands);
    return process;
  }


  @Override
  public boolean initMonitoring() {
    return false;
  }


  /*
   * Build the provider status, can be empty
   * @return the provider status - map of entries to add to the info section
   */
  @Override
  public Map<String, String> buildProviderStatus() {
    return new HashMap<String, String>();
  }

  public static class ProviderAbortable implements Abortable {
    @Override
    public void abort(String why, Throwable e) {
    }

    @Override
    public boolean isAborted() {
      return false;
    }
  }
  
  /* non-javadoc
   * @see org.apache.hoya.providers.ProviderService#buildMonitorDetails()
   */
  @Override
  public Map<String,URL> buildMonitorDetails(ClusterDescription clusterDesc) {
    return Collections.emptyMap();
  }
  
  protected String getInfoAvoidingNull(ClusterDescription clusterDesc, String key) {
    String value = clusterDesc.getInfo(key);

    return null == value ? "N/A" : value;
  }
}
