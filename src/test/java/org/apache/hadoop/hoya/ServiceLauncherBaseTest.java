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

package org.apache.hadoop.hoya;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.launcher.ServiceLauncher;
import org.junit.Assert;

/**
 * Base class for tests that use the service launcher
 */
public class ServiceLauncherBaseTest extends Assert {

  /**
   * Launch a service
   * @param serviceClass service class
   * @param conf configuration
   * @param args list of args to hand down (as both raw and processed)
   * @return the service launcher. It's exitCode field will
   * contain any exit code; its <code>service</code> field
   * the service itself.
   */
  public ServiceLauncher launch(Class serviceClass,
                                Configuration conf,
                                String... args) throws
                                    Throwable {
    ServiceLauncher serviceLauncher =
      new ServiceLauncher(serviceClass.getName());
      serviceLauncher.launchService(conf, args, false);
    return serviceLauncher;
  }

  /**
   * Launch a service
   * @param serviceClass service class
   * @param conf configuration
   * @param args list of args to hand down (as both raw and processed)
   */
  public void launchExpectingException(Class serviceClass,
                                       Configuration conf,
                                       String expectedText,
                                       String... args) throws
                                                       Throwable {
    ServiceLauncher serviceLauncher =
      new ServiceLauncher(serviceClass.getName());
    try {
      int result = serviceLauncher.launchService(conf, args, false);
      fail("Expected an exception with text containing " + expectedText
           + " -but the service completed with exit code " + result);
    } catch (Throwable thrown) {
      if (!thrown.toString().contains(expectedText)) {
        //not the right exception -rethrow
        throw thrown;
      }
    }
  }


}
