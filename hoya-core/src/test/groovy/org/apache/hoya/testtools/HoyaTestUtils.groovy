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

package org.apache.hoya.testtools

import org.apache.hadoop.fs.FileSystem as HadoopFS
import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.service.launcher.ServiceLaunchException
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher
import org.apache.hoya.api.ClusterDescription
import org.apache.hoya.api.ClusterNode
import org.apache.hoya.exceptions.BadClusterStateException
import org.apache.hoya.exceptions.HoyaException
import org.apache.hoya.exceptions.WaitTimeoutException
import org.apache.hoya.providers.hbase.HBaseKeys
import org.apache.hoya.tools.Duration
import org.apache.hoya.yarn.Arguments
import org.apache.hoya.yarn.client.HoyaClient
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.junit.Assert
import org.junit.Assume

import static org.apache.hoya.yarn.Arguments.ARG_OPTION

/**
 * Static utils for tests in this package and in other test projects.
 * 
 * It is designed to work with mini clusters as well as remote ones
 * 
 * This class is not final and may be extended for test cases.
 * 
 * Some of these methods are derived from the SwiftUtils and SwiftTestUtils
 * classes -replicated here so that they are available in Hadoop-2.0 code
 */
@Slf4j
@CompileStatic
class HoyaTestUtils extends Assert {

  public static void describe(String s) {
    log.info("");
    log.info("===============================");
    log.info(s);
    log.info("===============================");
    log.info("");
  }

  public static String prettyPrint(String json) {
    JsonOutput.prettyPrint(json)
  }

  public static void skip(String message) {
    log.warn("Skipping test: " + message)
    Assume.assumeTrue(message, false);
  }

  public static void assume(boolean condition, String message) {
    if (!condition) {
      log.warn("Skipping test: " + message)
      Assume.assumeTrue(message, false);
    }
  }


  public static void assertListEquals(List left, List right) {
    assert left.size() == right.size();
    for (int i = 0; i < left.size(); i++) {
      assert left[i] == right[i]
    }
  }

  /**
   * Assume that a string option is set and not equal to ""
   * @param conf configuration file
   * @param key key to look for
   */
  public static void assumeStringOptionSet(Configuration conf, String key) {
    if (!conf.getTrimmed(key)) {
      skip("Configuration key $key not set")
    }
  }
  
  
  /**
   * assert that a string option is set and not equal to ""
   * @param conf configuration file
   * @param key key to look for
   */
  public static void assertStringOptionSet(Configuration conf, String key) {
    getRequiredConfOption(conf, key)
  }
  
  

  /**
   * Assume that a boolean option is set and true.
   * Unset or false triggers a test skip
   * @param conf configuration file
   * @param key key to look for
   */
  public static void assumeBoolOptionTrue(Configuration conf, String key) {
    assumeBoolOption(conf, key, false)
  }

  /**
   * Assume that a boolean option is true.
   * False triggers a test skip
   * @param conf configuration file
   * @param key key to look for
   * @param defval default value if the property is not defined
   */
  public static void assumeBoolOption(
      Configuration conf, String key, boolean defval) {
    assume(conf.getBoolean(key, defval), 
      "Configuration key $key is false")
  }

  /**
   * Get a required config option (trimmed, incidentally).
   * Test will fail if not set
   * @param conf configuration
   * @param key key
   * @return the string
   */
  public static String getRequiredConfOption(Configuration conf, String key) {
    String val = conf.getTrimmed(key)
    if (!val) {
      fail("Missing configuration option $key")
    }
    return val;
  }

  /**
   * Wait for the cluster live; fail if it isn't within the (standard) timeout
   * @param hoyaClient client
   * @return the app report of the live cluster
   */
  public static ApplicationReport waitForClusterLive(HoyaClient hoyaClient,int goLiveTime) {
    ApplicationReport report = hoyaClient.monitorAppToRunning(
        new Duration(goLiveTime));
    assertNotNull(
        "Cluster did not go live in the time $goLiveTime",
        report);
    return report;
  }

  protected static String[] toArray(List<Object> args) {
    String[] converted = new String[args.size()];
    for (int i = 0; i < args.size(); i++) {
      def elt = args.get(i)
      assert args.get(i) != null
      converted[i] = elt.toString();
    }
    return converted;
  }

  public static void waitWhileClusterLive(HoyaClient client, int timeout) {
    Duration duration = new Duration(timeout);
    duration.start()
    while (client.actionExists(client.deployedClusterName, true) &&
           !duration.limitExceeded) {
      sleep(1000);
    }
    if (duration.limitExceeded) {
      fail("Cluster ${client.deployedClusterName} still live after $timeout ms")
    }
  }

  public static void waitUntilClusterLive(HoyaClient client, int timeout) {
    Duration duration = new Duration(timeout);
    duration.start()
    while (!client.actionExists(client.deployedClusterName, true) &&
           !duration.limitExceeded) {
      sleep(1000);
    }
    if (duration.limitExceeded) {
      fail("Cluster ${client.deployedClusterName} not live after $timeout ms")
    }
  }

  /**
   * Spin waiting for the Hoya role count to match expected
   * @param hoyaClient client
   * @param role role to look for
   * @param desiredCount RS count
   * @param timeout timeout
   */
  public static ClusterDescription waitForRoleCount(
      HoyaClient hoyaClient,
      String role,
      int desiredCount,
      int timeout) {
    return waitForRoleCount(hoyaClient, [(role): desiredCount], timeout)
  }

  /**
   * Spin waiting for the Hoya role count to match expected
   * @param hoyaClient client
   * @param roles map of roles to look for
   * @param desiredCount RS count
   * @param timeout timeout
   */
  public static ClusterDescription waitForRoleCount(
      HoyaClient hoyaClient,
      Map<String, Integer> roles,
      int timeout,
      String operation = "startup") {
    String clustername = hoyaClient.deployedClusterName;
    ClusterDescription status = null
    Duration duration = new Duration(timeout);
    duration.start()
    boolean roleCountFound = false;
    while (!roleCountFound) {
      StringBuilder details = new StringBuilder()

      boolean timedOut = duration.limitExceeded
      try {
        status = hoyaClient.getClusterDescription(clustername)
        roleCountFound = true;
        for (Map.Entry<String, Integer> entry : roles.entrySet()) {
          String role = entry.key
          int desiredCount = entry.value
          List<String> instances = status.instances[role]
          int instanceCount = instances != null ? instances.size() : 0;
          if (instanceCount != desiredCount) {
            roleCountFound = false;
          }
          details.append("[$role]: $instanceCount of $desiredCount; ")
        }
        if (roleCountFound) {
          //successful
          log.info("$operation: role count as desired: $details")
          break;
        }
      } catch (BadClusterStateException e) {
        // cluster not live yet; ignore or rethrow
        if (timedOut) {
          throw e;
        }
        details.append(e.toString());
      }
      if (timedOut) {
        duration.finish();
        describe("$operation: role count not met after $duration: $details")
        log.info(prettyPrint(status.toJsonString()))
        fail(
            "$operation: role counts not met  after $duration: $details in \n$status ")
      }
      log.debug("Waiting: " + details)
      Thread.sleep(1000)
    }
    return status
  }

  /**
   * Wait for the hbase master to be live (or past it in the lifecycle)
   * @param clustername cluster
   * @param spintime time to wait
   * @return true if the cluster came out of the sleep time live 
   * @throws IOException
   * @throws org.apache.hoya.exceptions.HoyaException
   */
  public static boolean spinForClusterStartup(HoyaClient hoyaClient, long spintime)
      throws WaitTimeoutException, IOException, HoyaException {
    int state = hoyaClient.waitForRoleInstanceLive(HBaseKeys.MASTER, spintime);
    return state == ClusterDescription.STATE_LIVE;
  }

  public static void dumpClusterStatus(HoyaClient hoyaClient, String text) {
    ClusterDescription status = hoyaClient.clusterDescription;
    dumpClusterDescription(text, status)
  }

  public static List<ClusterNode> listNodesInRole(HoyaClient hoyaClient, String role) {
    return hoyaClient.listClusterNodesInRole(role)
  }

  public static void dumpClusterDescription(String text, ClusterDescription status) {
    describe(text)
    log.info(prettyPrint(status.toJsonString()))
  }

  /**
   * Fetch the current site config from the Hoya AM, from the 
   * <code>clientProperties</code> field of the ClusterDescription
   * @param hoyaClient client
   * @param clustername name of the cluster
   * @return the site config
   */
  public static Configuration fetchClientSiteConfig(HoyaClient hoyaClient) {
    ClusterDescription status = hoyaClient.clusterDescription;
    Configuration siteConf = new Configuration(false)
    status.clientProperties.each { String key, String val ->
      siteConf.set(key, val, "hoya cluster");
    }
    return siteConf;
  }

  /**
   * Fetch a web page -the response code is not checked
   * @param url URL
   * @return the response body
   */

  public static String fetchWebPage(String url) {
    def client = new HttpClient(new MultiThreadedHttpConnectionManager());
    client.httpConnectionManager.params.connectionTimeout = 10000;
    GetMethod get = new GetMethod(url);

    get.followRedirects = true;
    int resultCode = client.executeMethod(get);
    String body = get.responseBodyAsString;
    return body;
  }

  /**
   * Assert that a service operation succeeded
   * @param service service
   */
  public static void assertSucceeded(ServiceLauncher service) {
    assert 0 == service.serviceExitCode;
  }

  /**
   * Make an assertion about the exit code of an exception
   * @param ex exception
   * @param exitCode exit code
   * @param text error text to look for in the exception
   */
  static void assertExceptionDetails(
      ServiceLaunchException ex,
      int exitCode,
      String text = "") {
    if (exitCode != ex.exitCode) {
      log.warn(
          "Wrong exit code, expected $exitCode but got $ex.exitCode in $ex",
          ex)
      assert exitCode == ex.exitCode
    }
    if (text) {
      if (!(ex.toString().contains(text))) {
        log.warn("String match failed in $ex", ex)
        assert ex.toString().contains(text);
      }
    }
  }

  /**
   * Launch the hoya client with the specific args; no validation
   * of return code takes place
   * @param conf configuration
   * @param args arg list
   * @return the return code
   */
  protected static ServiceLauncher<HoyaClient> execHoyaCommand(
      Configuration conf,
      List args) {
    ServiceLauncher<HoyaClient> serviceLauncher =
        new ServiceLauncher<HoyaClient>(HoyaClient.name);
    serviceLauncher.launchService(conf,
                                  toArray(args),
                                  false);
    return serviceLauncher
  }

  public static ServiceLauncher launch(Class serviceClass,
                                       Configuration conf,
                                       List<Object> args) throws
      Throwable {
    ServiceLauncher serviceLauncher =
        new ServiceLauncher(serviceClass.getName());
    serviceLauncher.launchService(conf,
                                  toArray(args),
                                  false);
    return serviceLauncher;
  }

  public static void launchExpectingException(Class serviceClass,
                                              Configuration conf,
                                              String expectedText,
                                              List args)
      throws Throwable {
    try {
      ServiceLauncher launch = launch(serviceClass, conf, args);
      fail("Expected an exception with text containing " + expectedText
               + " -but the service completed with exit code "
               + launch.getServiceExitCode());
    } catch (Throwable thrown) {
      if (!thrown.toString().contains(expectedText)) {
        //not the right exception -rethrow
        throw thrown;
      }
    }
  }


  public static ServiceLauncher<HoyaClient> launchHoyaClientAgainstRM(
      String address,
      List args,
      Configuration conf) {
    assert address != null
    log.info("Connecting to rm at ${address}")
    if (!args.contains(Arguments.ARG_MANAGER)) {
      args += [Arguments.ARG_MANAGER, address]
    }
    ServiceLauncher<HoyaClient> launcher = execHoyaCommand(conf, args)
    return launcher
  }

  /**
   * Add a configuration parameter as a cluster configuration option
   * @param extraArgs extra arguments
   * @param conf config
   * @param option option
   */
  public static void addClusterConfigOption(
      List<String> extraArgs,
      YarnConfiguration conf,
      String option) {
    
    conf.getTrimmed(option);
    extraArgs << ARG_OPTION << option << getRequiredConfOption(conf, option)
    
  }

  /**
   * Assert that a path refers to a directory
   * @param fs filesystem
   * @param path path of the directory
   * @throws IOException on File IO problems
   */
  public static void assertIsDirectory(HadoopFS fs,
                                       Path path) throws IOException {
    FileStatus fileStatus = fs.getFileStatus(path);
    assertIsDirectory(fileStatus);
  }

  /**
   * Assert that a path refers to a directory
   * @param fileStatus stats to check
   */
  public static void assertIsDirectory(FileStatus fileStatus) {
    assertTrue("Should be a dir -but isn't: " + fileStatus,
               fileStatus.isDirectory());
  }

  /**
   * Assert that a path exists -but make no assertions as to the
   * type of that entry
   *
   * @param fileSystem filesystem to examine
   * @param message message to include in the assertion failure message
   * @param path path in the filesystem
   * @throws IOException IO problems
   */
  public static void assertPathExists(
      HadoopFS fileSystem,
      String message,
      Path path) throws IOException {
    if (!fileSystem.exists(path)) {
      //failure, report it
      fail(message + ": not found \"" + path + "\" in " + path.getParent() + "-" +
        ls(fileSystem, path.getParent()));
    }
  }

  /**
   * Assert that a path does not exist
   *
   * @param fileSystem filesystem to examine
   * @param message message to include in the assertion failure message
   * @param path path in the filesystem
   * @throws IOException IO problems
   */
  public static void assertPathDoesNotExist(
      HadoopFS fileSystem,
      String message,
      Path path) throws IOException {
    try {
      FileStatus status = fileSystem.getFileStatus(path);
      fail(message + ": unexpectedly found " + path + " as  " + status);
    } catch (FileNotFoundException expected) {
      //this is expected

    }
  }

  /**
   * Assert that a FileSystem.listStatus on a dir finds the subdir/child entry
   * @param fs filesystem
   * @param dir directory to scan
   * @param subdir full path to look for
   * @throws IOException IO probles
   */
  public static void assertListStatusFinds(HadoopFS fs,
                                           Path dir,
                                           Path subdir) throws IOException {
    FileStatus[] stats = fs.listStatus(dir);
    boolean found = false;
    StringBuilder builder = new StringBuilder();
    for (FileStatus stat : stats) {
      builder.append(stat.toString()).append('\n');
      if (stat.getPath().equals(subdir)) {
        found = true;
      }
    }
    assertTrue("Path " + subdir
                   + " not found in directory " + dir + ":" + builder,
               found);
  }

  /**
   * List a a path to string
   * @param fileSystem filesystem
   * @param path directory
   * @return a listing of the filestatuses of elements in the directory, one
   * to a line, precedeed by the full path of the directory
   * @throws IOException connectivity problems
   */
  public static String ls(HadoopFS fileSystem, Path path)
  throws
      IOException {
    if (path == null) {
      //surfaces when someone calls getParent() on something at the top of the path
      return "/";
    }
    FileStatus[] stats;
    String pathtext = "ls " + path;
    try {
      stats = fileSystem.listStatus(path);
    } catch (FileNotFoundException e) {
      return pathtext + " -file not found";
    } catch (IOException e) {
      return pathtext + " -failed: " + e;
    }
    return pathtext + fileStatsToString(stats, "\n");
  }

  /**
   * Take an array of filestats and convert to a string (prefixed w/ a [01] counter
   * @param stats array of stats
   * @param separator separator after every entry
   * @return a stringified set
   */
  public static String fileStatsToString(FileStatus[] stats, String separator) {
    StringBuilder buf = new StringBuilder(stats.length * 128);
    for (int i = 0; i < stats.length; i++) {
      buf.append(String.format("[%02d] %s", i, stats[i])).append(separator);
    }
    return buf.toString();
  }

}
