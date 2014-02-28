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

package org.apache.hoya.exceptions;

import org.apache.hadoop.yarn.service.launcher.ServiceLaunchException;

import static org.apache.hoya.HoyaExitCodes.*;

public class HoyaException extends ServiceLaunchException {
  public HoyaException() {
    super(EXIT_EXCEPTION_THROWN, "HoyaException");
  }

  public HoyaException(int code, String message) {
    super(code, message);
  }

  public HoyaException(String s) {
    super(EXIT_EXCEPTION_THROWN, s);
  }

  public HoyaException(String s, Throwable throwable) {
    super(EXIT_EXCEPTION_THROWN, s, throwable);
  }

  /**
   * Format the exception as you create it
   * @param code
   * @param message
   * @param args
   */
  public HoyaException(int code, String message, Object ... args) {
    super(code, String.format(message, args));
  }

  /**
   * Format the exception, include a throwable. 
   * The throwable comes before the message so that it is out of the varargs
   * @param code exit code
   * @param throwable thrown
   * @param message message
   * @param args arguments
   */
  public HoyaException(int code,
                       Throwable throwable,
                       String message,
                       Object ... args) {
    super(code, String.format(message, args), throwable);
  }

}
