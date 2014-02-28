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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hoya.HoyaKeys;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.api.OptionKeys;
import org.apache.hoya.api.RoleKeys;
import org.apache.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hoya.exceptions.BadConfigException;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.exceptions.HoyaInternalStateException;
import org.apache.hoya.tools.HoyaFileSystem;
import org.apache.hoya.tools.HoyaUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * this is a factoring out of methods handy for providers. It's bonded to a log at
 * construction time
 */
public class ProviderUtils {

  protected final Logger log;

  /**
   * Create an instace
   * @param log log directory to use -usually the provider
   */
  
  public ProviderUtils(Logger log) {
    this.log = log;
  }

  /**
   * Add a set of dependencies to the provider resources being built up,
   * by copying them from the local classpath to the remote one, then
   * registering them
   * @param providerResources map of provider resources to add these entries to
   * @param hoyaFileSystem target filesystem
   * @param tempPath path in the cluster FS for temp files
   * @param libdir relative directory to place resources
   * @param resources list of resource names (e.g. "hbase.jar"
   * @param classes list of classes where classes[i] refers to a class in
   * resources[i]
   * @throws IOException IO problems
   * @throws HoyaException any Hoya problem
   */
  public static void addDependencyJars(Map<String, LocalResource> providerResources,
                                       HoyaFileSystem hoyaFileSystem,
                                       Path tempPath,
                                       String libdir,
                                       String[] resources,
                                       Class<?>[] classes
                                      ) throws
                                        IOException,
                                        HoyaException {
    if (resources.length != classes.length) {
      throw new HoyaInternalStateException(
        "mismatch in Jar names [%d] and classes [%d]",
        resources.length,
        classes.length);
    }
    int size = resources.length;
    for (int i = 0; i < size; i++) {
      String jarName = resources[i];
      Class<?> clazz = classes[i];
      HoyaUtils.putJar(providerResources,
                       hoyaFileSystem,
                       clazz,
                       tempPath,
                       libdir,
                       jarName);
    }
    
  }

  public int validateAndGetYARNMemory(Map<String, String> role) throws
                                                             BadConfigException {
    return HoyaUtils.getIntValue(role, RoleKeys.YARN_MEMORY, 0, 0, -1);
  }


  /**
   * build the log directory
   * @return the log dir
   */
  public String getLogdir() throws IOException {
    String logdir = System.getenv("LOGDIR");
    if (logdir == null) {
      logdir =
        HoyaKeys.HOYA_TMP_LOGDIR_PREFIX + UserGroupInformation.getCurrentUser().getShortUserName();
    }
    return logdir;
  }


  /**
   * Validate the node count and heap size values of a node class 
   *
   * @param name node class name
   * @param count requested node count
   * @param min requested heap size
   * @param max
   * @throws BadCommandArgumentsException if the values are out of range
   */
  public void validateNodeCount(String name,
                                int count,
                                int min,
                                int max) throws BadCommandArgumentsException {
    if (count < min) {
      throw new BadCommandArgumentsException(
        "requested no of %s nodes: %d is below the minimum of %d", name, count,
        min);
    }
    if (max > 0 && count > max) {
      throw new BadCommandArgumentsException(
        "requested no of %s nodes: %d is above the maximum of %d", name, count,
        max);
    }
  }

  /**
   * copy all options beginning site. into the site.xml
   * @param clusterSpec cluster specification
   * @param sitexml map for XML file to build up
   */
  public void propagateSiteOptions(ClusterDescription clusterSpec,
                                    Map<String, String> sitexml) {
    Map<String, String> options = clusterSpec.options;
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(OptionKeys.SITE_XML_PREFIX)) {
        String envName = key.substring(OptionKeys.SITE_XML_PREFIX.length());
        if (!envName.isEmpty()) {
          sitexml.put(envName, entry.getValue());
        }
      }
    }
  }

  /**
   * Propagate an option from the cluster specification option map
   * to the site XML map, using the site key for the name
   * @param clusterSpec cluster specification
   * @param optionKey key in the option map
   * @param sitexml  map for XML file to build up
   * @param siteKey key to assign the value to in the site XML
   * @throws BadConfigException if the option is missing from the cluster spec
   */
  public void propagateOption(ClusterDescription clusterSpec,
                              String optionKey,
                              Map<String, String> sitexml,
                              String siteKey) throws BadConfigException {
    sitexml.put(siteKey, clusterSpec.getMandatoryOption(optionKey));
  }
  
  /**
   * Build the image dir. This path is relative and only valid at the far end
   * @param clusterSpec cluster spec
   * @param archiveSubdir subdir
   * @return a relative path to the tar
   */
  public File buildImageDir(ClusterDescription clusterSpec,
                                   String archiveSubdir) {
    File basedir;
    if (clusterSpec.isImagePathSet()) {
      basedir = new File(new File(HoyaKeys.LOCAL_TARBALL_INSTALL_SUBDIR),
                         archiveSubdir);
    } else {
      basedir = new File(clusterSpec.getApplicationHome());
    }
    return basedir;
  }


  /**
   * Build the image dir. This path is relative and only valid at the far end
   * @param clusterSpec cluster spec
   * @param bindir bin subdir
   * @param script script in bin subdir
   * @return the path to the script
   * @throws FileNotFoundException if a file is not found, or it is not a directory* 
   */
  public String buildPathToHomeDir(ClusterDescription clusterSpec,
                                  String bindir,
                                  String script) throws FileNotFoundException {
    String path;
    File scriptFile;
    if (clusterSpec.isImagePathSet()) {
      File tarball = new File(HoyaKeys.LOCAL_TARBALL_INSTALL_SUBDIR);
      scriptFile = findBinScriptInExpandedArchive(tarball, bindir, script);
      // now work back from the script to build the relative path
      // to the binary which will be valid remote or local
      StringBuilder builder = new StringBuilder();
      builder.append(HoyaKeys.LOCAL_TARBALL_INSTALL_SUBDIR);
      builder.append("/");
      //for the script, we want the name of ../..
      File archive = scriptFile.getParentFile().getParentFile();
      builder.append(archive.getName());
      path = builder.toString();

    } else {
      // using a home directory which is required to be present on 
      // the local system -so will be absolute and resolvable
      File homedir = new File(clusterSpec.getApplicationHome());
      path = homedir.getAbsolutePath();

      //this is absolute, resolve its entire path
      HoyaUtils.verifyIsDir(homedir, log);
      File bin = new File(homedir, bindir);
      HoyaUtils.verifyIsDir(bin, log);
      scriptFile = new File(bin, script);
      HoyaUtils.verifyFileExists(scriptFile, log);
    }
    return path;
  }

  /**
   * Build the image dir. This path is relative and only valid at the far end
   * @param clusterSpec cluster spec
   * @param bindir bin subdir
   * @param script script in bin subdir
   * @return the path to the script
   * @throws FileNotFoundException if a file is not found, or it is not a directory* 
   */
  public String buildPathToScript(ClusterDescription clusterSpec,
                                String bindir,
                                String script) throws FileNotFoundException {
    String homedir = buildPathToHomeDir(clusterSpec, bindir, script);
    StringBuilder builder = new StringBuilder(homedir);
    builder.append("/");
    builder.append(bindir);
    builder.append("/");
    builder.append(script);
    return builder.toString();
  }
  
  

  public static String convertToAppRelativePath(File file) {
    return convertToAppRelativePath(file.getPath());
  }

  public static String convertToAppRelativePath(String path) {
    return ApplicationConstants.Environment.PWD.$() + "/" + path;
  }


  public static void validatePathReferencesLocalDir(String meaning, String path)
      throws BadConfigException {
    File file = new File(path);
    if (!file.exists()) {
      throw new BadConfigException("%s directory %s not found", meaning, file);
    }
    if (!file.isDirectory()) {
      throw new BadConfigException("%s is not a directory: %s", meaning, file);
    }
  }

  /**
   * get the user name
   * @return the user name
   */
  public String getUserName() throws IOException {
    return UserGroupInformation.getCurrentUser().getShortUserName();
  }

  /**
   * Find a script in an expanded archive
   * @param base base directory
   * @param bindir bin subdir
   * @param script script in bin subdir
   * @return the path to the script
   * @throws FileNotFoundException if a file is not found, or it is not a directory
   */
  public File findBinScriptInExpandedArchive(File base,
                                             String bindir,
                                             String script)
      throws FileNotFoundException {
    
    HoyaUtils.verifyIsDir(base, log);
    File[] ls = base.listFiles();
    if (ls == null) {
      //here for the IDE to be happy, as the previous check will pick this case
      throw new FileNotFoundException("Failed to list directory " + base);
    }

    log.debug("Found {} entries in {}", ls.length, base);
    List<File> directories = new LinkedList<File>();
    StringBuilder dirs = new StringBuilder();
    for (File file : ls) {
      log.debug("{}", false);
      if (file.isDirectory()) {
        directories.add(file);
        dirs.append(file.getPath()).append(" ");
      }
    }
    if (directories.size() > 1) {
      throw new FileNotFoundException(
        "Too many directories in archive to identify binary: " + dirs);
    }
    if (directories.isEmpty()) {
      throw new FileNotFoundException(
        "No directory found in archive " + base);
    }
    File archive = directories.get(0);
    File bin = new File(archive, bindir);
    HoyaUtils.verifyIsDir(bin, log);
    File scriptFile = new File(bin, script);
    HoyaUtils.verifyFileExists(scriptFile, log);
    return scriptFile;
  }

  /**
   * Return any additional arguments (argv) to provide when starting this role
   * 
   * @param roleOptions
   *          The options for this role
   * @return A non-null String which contains command line arguments for this role, or the empty string.
   */
  public static String getAdditionalArgs(Map<String,String> roleOptions) {
    if (roleOptions.containsKey(RoleKeys.ROLE_ADDITIONAL_ARGS)) {
      String additionalArgs = roleOptions.get(RoleKeys.ROLE_ADDITIONAL_ARGS);
      if (null != additionalArgs) {
        return additionalArgs;
      }
    }

    return "";
  }
}
