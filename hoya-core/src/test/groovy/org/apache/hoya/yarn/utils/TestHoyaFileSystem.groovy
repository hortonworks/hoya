/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hoya.yarn.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hoya.HoyaXmlConfKeys
import org.apache.hoya.tools.HoyaFileSystem
import org.apache.hoya.yarn.HoyaTestBase
import org.junit.Test

import static org.junit.Assert.assertEquals

class TestHoyaFileSystem extends HoyaTestBase {
  private static Configuration defaultConfiguration() {
    new Configuration()
  }

  private static Configuration createConfigurationWithKV(String key, String value) {
    def conf = defaultConfiguration()
    conf.set(key, value)
    conf
  }

  @Test
  public void testHoyaBasePathDefaultValue() throws Throwable {
    Configuration configuration = defaultConfiguration()
    FileSystem fileSystem = FileSystem.get(configuration)
    assertEquals new HoyaFileSystem(fileSystem, configuration).getBaseHoyaPath(), new Path(fileSystem.getHomeDirectory(), ".hoya")
  }

  @Test
  public void testHoyaBasePathCustomValue() throws Throwable {
    Configuration configuration = createConfigurationWithKV(HoyaXmlConfKeys.KEY_BASE_HOYA_PATH, "/hoya/cluster")
    FileSystem fileSystem = FileSystem.get(configuration)
    assertEquals new HoyaFileSystem(fileSystem, configuration).getBaseHoyaPath(), new Path("/hoya/cluster")
  }

  @Test
  public void testPurgeTempDir() throws Throwable {
    String CLUSTER1 = "cluster1"

    Configuration configuration = new Configuration()
    FileSystem fs = FileSystem.get(new URI("file:///"), configuration)
    HoyaFileSystem hoyaFileSystem = new HoyaFileSystem(fs, configuration)
    Path inst = hoyaFileSystem.createHoyaAppInstanceTempPath(CLUSTER1, "001")

    assertTrue(fs.exists(inst))
    hoyaFileSystem.purgeHoyaAppInstanceTempFiles(CLUSTER1)
    assertFalse(fs.exists(inst))
  }

}
