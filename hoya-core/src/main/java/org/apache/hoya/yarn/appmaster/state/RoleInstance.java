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

package org.apache.hoya.yarn.appmaster.state;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hoya.api.proto.Messages;
import org.apache.hoya.tools.HoyaUtils;

import java.util.Arrays;

/**
 * Info about a continer to keep around when deciding which container to release
 */
public final class RoleInstance implements Cloneable {

  public Container container;
  /**
   * UUID of container used in Hoya RPC to refer to instances. 
   * The string value of the container ID is used here.
   */
  public final String uuid;
  public long createTime;
  public long startTime;
  /**
   * flag set when it is released, to know if it has
   * already been targeted for termination
   */
  public boolean released;
  public String role;
  public int roleId;
  /**
   * state from {@link org.apache.hoya.api.ClusterDescription}
   */
  public int state;

  /**
   * Exit code: only valid if the state >= STOPPED
   */
  public int exitCode;

  /**
   * what was the command executed?
   */
  public String command;

  /**
   * Any diagnostics
   */
  public String diagnostics;

  /**
   * What is the tail output from the executed process (or [] if not started
   * or the log cannot be picked up
   */
  public String[] output;

  /**
   * Any environment details
   */
  public String[] environment;

  public RoleInstance(Container container) {
    this.container = container;
    if (container == null) {
      throw new NullPointerException("Null container");
    }
    if (container.getId() == null) {
      throw new NullPointerException("Null container ID");
    }
    uuid = container.getId().toString();
  }

  public ContainerId getId() {
    return container.getId();
  }
  
  public NodeId getNodeId() {
    return container.getNodeId();
  }

  @Override
  public String toString() {
    final StringBuilder sb =
      new StringBuilder("RoleInstance{");
    sb.append("container=").append(HoyaUtils.containerToString(container));
    sb.append(", uuid='").append(uuid).append('\'');
    sb.append(", createTime=").append(createTime);
    sb.append(", startTime=").append(startTime);
    sb.append(", released=").append(released);
    sb.append(", role='").append(role).append('\'');
    sb.append(", roleId=").append(roleId);
    sb.append(", state=").append(state);
    sb.append(", exitCode=").append(exitCode);
    sb.append(", command='").append(command).append('\'');
    sb.append(", diagnostics='").append(diagnostics).append('\'');
    sb.append(", output=").append(Arrays.toString(output));
    sb.append(", environment=").append(Arrays.toString(environment));
    sb.append('}');
    return sb.toString();
  }

  public ContainerId getContainerId() {
    return container != null ? container.getId() : null;
  }

  /**
   * Generate the protobuf format of a request
   * @return protobuf format. This excludes the Container info
   */
  public Messages.RoleInstanceState toProtobuf() {
    Messages.RoleInstanceState.Builder builder =
      Messages.RoleInstanceState.newBuilder();
    if (container != null) {
      builder.setName(container.getId().toString());
    } else {
      builder.setName("unallocated instance");
    }
    if (command != null) {
      builder.setCommand(command);
    }
    if (environment != null) {
      builder.addAllEnvironment(Arrays.asList(environment));
    }
    if (diagnostics != null) {
      builder.setDiagnostics(diagnostics);
    }
    builder.setExitCode(exitCode);

    if (output != null) {
      builder.addAllOutput(Arrays.asList(output));
    }
    if (role != null) {
      builder.setRole(role);
    }
    builder.setRoleId(roleId);
    builder.setState(state);
    if (uuid != null) {
      builder.setUuid(uuid);
    }
    return builder.build();
  }

  /**
   * Clone operation clones all the simple values but shares the 
   * Container object into the cloned copy -same with the output,
   * diagnostics and env arrays.
   * @return a clone of the object
   * @throws CloneNotSupportedException
   */
  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }
}
