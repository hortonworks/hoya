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

package org.apache.hoya.tools;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hoya.HoyaExitCodes;
import org.apache.hoya.HoyaKeys;
import org.apache.hoya.HoyaXmlConfKeys;
import org.apache.hoya.api.ClusterDescription;
import org.apache.hoya.exceptions.BadClusterStateException;
import org.apache.hoya.exceptions.ErrorStrings;
import org.apache.hoya.exceptions.HoyaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class HoyaFileSystem {

  private static final Logger log = LoggerFactory.getLogger(HoyaFileSystem.class);

  private final FileSystem fileSystem;
  private final Configuration configuration;

  public HoyaFileSystem(FileSystem fileSystem, Configuration configuration) {
    Preconditions.checkNotNull(fileSystem, "Cannot create a HoyaFileSystem with a null FileSystem");
    Preconditions.checkNotNull(configuration, "Cannot create a HoyaFileSystem with a null Configuration");
    this.fileSystem = fileSystem;
    this.configuration = configuration;
  }

  public HoyaFileSystem(Configuration configuration) throws IOException {
    Preconditions.checkNotNull(configuration, "Cannot create a HoyaFileSystem with a null Configuration");
    this.fileSystem = FileSystem.get(configuration);
    this.configuration = fileSystem.getConf();
  }

  /**
   * Get the temp path for this cluster
   * @param clustername name of the cluster
   * @return path for temp files (is not purged)
   */
  public Path getTempPathForCluster(String clustername) {
    Path clusterDir = buildHoyaClusterDirPath(clustername);
    return new Path(clusterDir, HoyaKeys.HOYA_TMP_DIR_PREFIX);
  }

  /**
   * Returns the underlying FileSystem for this object.
   *
   * @return filesystem
   */
  public FileSystem getFileSystem() {
    return fileSystem;
  }

  /**
   * Build up the path string for a cluster instance -no attempt to
   * create the directory is made
   *
   * @param clustername name of the cluster
   * @return the path for persistent data
   */
  public Path buildHoyaClusterDirPath(String clustername) {
    if (clustername == null) {
      throw new NullPointerException();
    }
    Path hoyaPath = getBaseHoyaPath();
    return new Path(hoyaPath, HoyaKeys.CLUSTER_DIRECTORY + "/" + clustername);
  }

  /**
   * Create the Hoya cluster path for a named cluster and all its subdirs
   * This is a directory; a mkdirs() operation is executed
   * to ensure that it is there.
   *
   * @param clustername name of the cluster
   * @return the path to the cluster directory
   * @throws java.io.IOException                      trouble
   * @throws org.apache.hoya.exceptions.HoyaException hoya-specific exceptions
   */
  public Path createClusterDirectories(String clustername, Configuration conf) throws
          IOException,
          HoyaException {
    Path clusterDirectory = buildHoyaClusterDirPath(clustername);
    Path snapshotConfPath =
            new Path(clusterDirectory, HoyaKeys.SNAPSHOT_CONF_DIR_NAME);
    Path generatedConfPath =
            new Path(clusterDirectory, HoyaKeys.GENERATED_CONF_DIR_NAME);
    Path historyPath =
            new Path(clusterDirectory, HoyaKeys.HISTORY_DIR_NAME);
    String clusterDirPermsOct = conf.get(HoyaXmlConfKeys.HOYA_CLUSTER_DIRECTORY_PERMISSIONS,
            HoyaXmlConfKeys.DEFAULT_HOYA_CLUSTER_DIRECTORY_PERMISSIONS);
    FsPermission clusterPerms = new FsPermission(clusterDirPermsOct);

    verifyClusterDirectoryNonexistent(clustername, clusterDirectory);


    createWithPermissions(clusterDirectory, clusterPerms);
    createWithPermissions(snapshotConfPath, clusterPerms);
    createWithPermissions(generatedConfPath, clusterPerms);
    createWithPermissions(historyPath, clusterPerms);

    // Data Directory
    Path datapath = new Path(clusterDirectory, HoyaKeys.DATA_DIR_NAME);
    String dataOpts =
            conf.get(HoyaXmlConfKeys.HOYA_DATA_DIRECTORY_PERMISSIONS,
                    HoyaXmlConfKeys.DEFAULT_HOYA_DATA_DIRECTORY_PERMISSIONS);
    HoyaFileSystem.log.debug("Setting data directory permissions to {}", dataOpts);
    createWithPermissions(datapath, new FsPermission(dataOpts));

    return clusterDirectory;
  }

  /**
   * Create a directory with the given permissions.
   *
   * @param dir          directory
   * @param clusterPerms cluster permissions
   * @throws java.io.IOException                                 IO problem
   * @throws org.apache.hoya.exceptions.BadClusterStateException any cluster state problem
   */
  public void createWithPermissions(Path dir, FsPermission clusterPerms) throws
          IOException,
          BadClusterStateException {
    if (fileSystem.isFile(dir)) {
      // HADOOP-9361 shows some filesystems don't correctly fail here
      throw new BadClusterStateException(
              "Cannot create a directory over a file %s", dir);
    }
    HoyaFileSystem.log.debug("mkdir {} with perms {}", dir, clusterPerms);
    //no mask whatoever
    fileSystem.getConf().set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "000");
    fileSystem.mkdirs(dir, clusterPerms);
    //and force set it anyway just to make sure
    fileSystem.setPermission(dir, clusterPerms);
  }

  /**
   * Get the permissions of a path
   *
   * @param path path to check
   * @return the permissions
   * @throws java.io.IOException any IO problem (including file not found)
   */
  public FsPermission getPathPermissions(Path path) throws IOException {
    FileStatus status = fileSystem.getFileStatus(path);
    return status.getPermission();
  }

  /**
   * Verify that the cluster directory is not present
   *
   * @param clustername      name of the cluster
   * @param clusterDirectory actual directory to look for
   * @return the path to the cluster directory
   * @throws java.io.IOException                      trouble with FS
   * @throws org.apache.hoya.exceptions.HoyaException If the directory exists
   */
  public void verifyClusterDirectoryNonexistent(String clustername,
                                                Path clusterDirectory) throws
          IOException,
          HoyaException {
    if (fileSystem.exists(clusterDirectory)) {
      throw new HoyaException(HoyaExitCodes.EXIT_CLUSTER_EXISTS,
              ErrorStrings.PRINTF_E_ALREADY_EXISTS, clustername,
              clusterDirectory);
    }
  }

  /**
   * Verify that a user has write access to a directory.
   * It does this by creating then deleting a temp file
   *
   * @param dirPath actual directory to look for
   * @throws FileNotFoundException file not found
   * @throws IOException  trouble with FS
   * @throws BadClusterStateException if the directory is not writeable
   */
  public void verifyDirectoryWriteAccess(Path dirPath) throws IOException, HoyaException {
    verifyPathExists(dirPath);
    Path tempFile = new Path(dirPath, "tmp-file-for-checks");
    try {
      FSDataOutputStream out = null;
      out = fileSystem.create(tempFile, true);
      IOUtils.closeStream(out);
      fileSystem.delete(tempFile, false);
    } catch (IOException e) {
      HoyaFileSystem.log.warn("Failed to create file {}: {}", tempFile, e);
      throw new BadClusterStateException(e,
              "Unable to write to directory %s : %s", dirPath, e.toString());
    }
  }

  /**
   * Verify that a path exists
   * @param path path to check
   * @throws FileNotFoundException file not found
   * @throws IOException  trouble with FS
   */
  public void verifyPathExists(Path path) throws IOException {
    if (!fileSystem.exists(path)) {
      throw new FileNotFoundException(path.toString());
    }
  }

  /**
   * Verify that a path exists
   * @param path path to check
   * @throws FileNotFoundException file not found or is not a file
   * @throws IOException  trouble with FS
   */
  public void verifyFileExists(Path path) throws IOException {
    FileStatus status = fileSystem.getFileStatus(path);

    if (!status.isFile()) {
      throw new FileNotFoundException("Not a file: " + path.toString());
    }
  }

  /**
   * Create the application-instance specific temporary directory
   * in the DFS
   *
   * @param clustername name of the cluster
   * @param subdir       application ID
   * @return the path; this directory will already have been created
   */
  public Path createHoyaAppInstanceTempPath(String clustername,
                                            String subdir) throws
          IOException {
    Path tmp = getTempPathForCluster(clustername);
    Path instancePath = new Path(tmp, subdir);
    fileSystem.mkdirs(instancePath);
    return instancePath;
  }

  /**
   * Create the application-instance specific temporary directory
   * in the DFS
   *
   * @param clustername name of the cluster
   * @return the path; this directory will already have been deleted
   */
  public Path purgeHoyaAppInstanceTempFiles(String clustername) throws
          IOException {
    Path tmp = getTempPathForCluster(clustername);
    fileSystem.delete(tmp, true);
    return tmp;
  }

  /**
   * Get the base path for hoya
   *
   * @return the base path optionally configured by {@value HoyaXmlConfKeys#KEY_BASE_HOYA_PATH}
   */
  public Path getBaseHoyaPath() {
    String configuredHoyaBasePath = configuration.get(HoyaXmlConfKeys.KEY_BASE_HOYA_PATH);
    return configuredHoyaBasePath != null ? new Path(configuredHoyaBasePath) :
           new Path(getHomeDirectory(), ".hoya");
  }

  public Path getHomeDirectory() {
    return fileSystem.getHomeDirectory();
  }

  /**
   * Overwrite a cluster specification. This code
   * attempts to do this atomically by writing the updated specification
   * to a new file, renaming the original and then updating the original.
   * There's special handling for one case: the original file doesn't exist
   *
   * @param clusterSpec
   * @param clusterDirectory
   * @param clusterSpecPath
   * @return true if the original cluster specification was updated.
   */
  public boolean updateClusterSpecification(Path clusterDirectory,
                                            Path clusterSpecPath,
                                            ClusterDescription clusterSpec) throws IOException {

    //it is not currently there -do a write with overwrite disabled, so that if
    //it appears at this point this is picked up
    if (!fileSystem.exists(clusterSpecPath) &&
            writeSpecWithoutOverwriting(clusterSpecPath, clusterSpec)) {
      return true;
    }

    //save to a renamed version
    String specTimestampedFilename = "spec-" + System.currentTimeMillis();
    Path specSavePath =
            new Path(clusterDirectory, specTimestampedFilename + ".json");
    Path specOrigPath =
            new Path(clusterDirectory, specTimestampedFilename + "-orig.json");

    //roll the specification. The (atomic) rename may fail if there is
    //an overwrite, which is how we catch re-entrant calls to this
    if (!writeSpecWithoutOverwriting(specSavePath, clusterSpec)) {
      return false;
    }
    if (!fileSystem.rename(clusterSpecPath, specOrigPath)) {
      return false;
    }
    try {
      if (!fileSystem.rename(specSavePath, clusterSpecPath)) {
        return false;
      }
    } finally {
      if (!fileSystem.delete(specOrigPath, false)) {
        return false;
      }
    }
    return true;
  }

  public boolean writeSpecWithoutOverwriting(Path clusterSpecPath,
                                             ClusterDescription clusterSpec) {
    try {
      clusterSpec.save(fileSystem, clusterSpecPath, false);
    } catch (IOException e) {
      HoyaFileSystem.log.debug("Failed to save cluster specification -race condition? " + e,
              e);
      return false;
    }
    return true;
  }

  public boolean maybeAddImagePath(Map<String, LocalResource> localResources,
                                   Path imagePath) throws IOException {
    if (imagePath != null) {
      LocalResource resource = createAmResource(imagePath,
              LocalResourceType.ARCHIVE);
      localResources.put(HoyaKeys.LOCAL_TARBALL_INSTALL_SUBDIR, resource);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Create an AM resource from the
   *
   * @param destPath     dest path in filesystem
   * @param resourceType resource type
   * @return the resource set up wih application-level visibility and the
   * timestamp & size set from the file stats.
   */
  public LocalResource createAmResource(Path destPath, LocalResourceType resourceType) throws IOException {
    FileStatus destStatus = fileSystem.getFileStatus(destPath);
    LocalResource amResource = Records.newRecord(LocalResource.class);
    amResource.setType(resourceType);
    // Set visibility of the resource
    // Setting to most private option
    amResource.setVisibility(LocalResourceVisibility.APPLICATION);
    // Set the resource to be copied over
    amResource.setResource(ConverterUtils.getYarnUrlFromPath(destPath));
    // Set timestamp and length of file so that the framework
    // can do basic sanity checks for the local resource
    // after it has been copied over to ensure it is the same
    // resource the client intended to use with the application
    amResource.setTimestamp(destStatus.getModificationTime());
    amResource.setSize(destStatus.getLen());
    return amResource;
  }

  /**
   * Register all files under a fs path as a directory to push out
   *
   * @param srcDir          src dir
   * @param destRelativeDir dest dir (no trailing /)
   * @return the map of entries
   */
  public Map<String, LocalResource> submitDirectory(Path srcDir, String destRelativeDir) throws IOException {
    //now register each of the files in the directory to be
    //copied to the destination
    FileStatus[] fileset = fileSystem.listStatus(srcDir);
    Map<String, LocalResource> localResources =
            new HashMap<String, LocalResource>(fileset.length);
    for (FileStatus entry : fileset) {

      LocalResource resource = createAmResource(entry.getPath(),
              LocalResourceType.FILE);
      String relativePath = destRelativeDir + "/" + entry.getPath().getName();
      localResources.put(relativePath, resource);
    }
    return localResources;
  }

  /**
   * Perform any post-load cluster validation. This may include loading
   * a provider and having it check it
   *
   * @param clusterSpecPath path to cspec
   * @param clusterSpec     the cluster spec to validate
   */
  public void verifySpecificationValidity(Path clusterSpecPath,
                                          ClusterDescription clusterSpec) throws HoyaException {
    if (clusterSpec.state == ClusterDescription.STATE_INCOMPLETE) {
      throw new BadClusterStateException(ErrorStrings.E_INCOMPLETE_CLUSTER_SPEC + clusterSpecPath);
    }
  }

  /**
   * Load a cluster spec then validate it
   *
   * @param clusterSpecPath path to cspec
   * @return the cluster spec
   * @throws java.io.IOException                      IO problems
   * @throws org.apache.hoya.exceptions.HoyaException cluster location, spec problems
   */
  public ClusterDescription loadAndValidateClusterSpec(Path clusterSpecPath) throws IOException, HoyaException {
    ClusterDescription clusterSpec =
            ClusterDescription.load(fileSystem, clusterSpecPath);
    //spec is loaded, just look at its state;
    verifySpecificationValidity(clusterSpecPath, clusterSpec);
    return clusterSpec;
  }

  /**
   * Locate a cluster specification in the FS. This includes a check to verify
   * that the file is there.
   *
   * @param clustername name of the cluster
   * @return the path to the spec.
   * @throws java.io.IOException                      IO problems
   * @throws org.apache.hoya.exceptions.HoyaException if the path isn't there
   */
  public Path locateClusterSpecification(String clustername) throws IOException, HoyaException {
    Path clusterDirectory = buildHoyaClusterDirPath(clustername);
    Path clusterSpecPath =
            new Path(clusterDirectory, HoyaKeys.CLUSTER_SPECIFICATION_FILE);
    ClusterDescription.verifyClusterSpecExists(clustername, fileSystem,
            clusterSpecPath);
    return clusterSpecPath;
  }

  /**
   * Submit a JAR containing a specific class, returning
   * the resource to be mapped in
   *
   * @param clazz   class to look for
   * @param subdir  subdirectory (expected to end in a "/")
   * @param jarName <i>At the destination</i>
   * @return the local resource ref
   * @throws java.io.IOException trouble copying to HDFS
   */
  public LocalResource submitJarWithClass(Class<?> clazz, Path tempPath, String subdir, String jarName)
          throws IOException, HoyaException {
    File localFile = HoyaUtils.findContainingJar(clazz);
    if (null == localFile) {
      throw new FileNotFoundException("Could not find JAR containing " + clazz);
    }

    LocalResource resource = submitFile(localFile, tempPath, subdir, jarName);
    return resource;
  }

  /**
   * Submit a local file to the filesystem references by the instance's cluster
   * filesystem
   *
   * @param localFile    filename
   * @param subdir       subdirectory (expected to end in a "/")
   * @param destFileName destination filename
   * @return the local resource ref
   * @throws java.io.IOException trouble copying to HDFS
   */
  private LocalResource submitFile(File localFile, Path tempPath, String subdir, String destFileName) throws IOException {
    Path src = new Path(localFile.toString());
    Path subdirPath = new Path(tempPath, subdir);
    fileSystem.mkdirs(subdirPath);
    Path destPath = new Path(subdirPath, destFileName);

    fileSystem.copyFromLocalFile(false, true, src, destPath);

    // Set the type of resource - file or archive
    // archives are untarred at destination
    // we don't need the jar file to be untarred for now
    return createAmResource(destPath, LocalResourceType.FILE);
  }

  /**
   * list entries in a filesystem directory
   *
   * @param path directory
   * @return a listing, one to a line
   * @throws java.io.IOException
   */
  public String listFSDir(Path path) throws IOException {
    FileStatus[] stats = fileSystem.listStatus(path);
    StringBuilder builder = new StringBuilder();
    for (FileStatus stat : stats) {
      builder.append(stat.getPath().toString())
              .append("\t")
              .append(stat.getLen())
              .append("\n");
    }
    return builder.toString();
  }

  public void touch(Path path) throws IOException {
    FSDataOutputStream out = fileSystem.create(path);
    out.close();
  }

  public void cat(Path path, String data) throws IOException {
    FSDataOutputStream out = fileSystem.create(path);
    byte[] bytes = data.getBytes(Charset.forName("UTF-8"));
    out.write(bytes);
    out.close();
  }
}
