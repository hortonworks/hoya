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

package org.apache.hoya.yarn.appmaster;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hoya.yarn.appmaster.state.RMOperationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hands off RM operations to the Resource Manager
 */
public class AsyncRMOperationHandler extends RMOperationHandler {
  protected static final Logger log =
    LoggerFactory.getLogger(AsyncRMOperationHandler.class);
  private final AMRMClientAsync<AMRMClient.ContainerRequest> client;

  public AsyncRMOperationHandler(AMRMClientAsync<AMRMClient.ContainerRequest> client) {
    this.client = client;
  }

  @Override
  public void releaseAssignedContainer(ContainerId containerId) {
    log.debug("Releasing container {}", containerId);

    client.releaseAssignedContainer(containerId);
  }

  @Override
  public void addContainerRequest(AMRMClient.ContainerRequest req) {
    client.addContainerRequest(req);
  }
}
