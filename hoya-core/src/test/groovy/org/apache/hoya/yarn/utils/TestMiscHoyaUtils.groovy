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

package org.apache.hoya.yarn.utils

import org.apache.hoya.tools.HoyaUtils
import org.apache.hoya.yarn.HoyaTestBase
import org.junit.Test

class TestMiscHoyaUtils extends HoyaTestBase {

  @Test
  public void testToGMTString() {
    long timestamp = 123456789

    String s1 = new Date(timestamp).toGMTString();
    String s2 = HoyaUtils.toGMTString(timestamp)

    assertEquals(s1, s2)
  }
}
