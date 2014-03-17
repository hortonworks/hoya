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

package org.apache.hoya.yarn.params;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hoya.HoyaXmlConfKeys;
import org.apache.hoya.exceptions.BadCommandArgumentsException;
import org.apache.hoya.exceptions.ErrorStrings;
import org.apache.hoya.exceptions.HoyaException;
import org.apache.hoya.yarn.HoyaActions;

import java.util.Collection;

/**
 * Hoya Client CLI Args
 */

public class ClientArgs extends CommonArgs {

  /*
   
   All the arguments for specific actions
  
   */
  /**
   * This is not bonded to jcommander, it is set up
   * after the construction to point to the relevant
   * entry
   */
  private AbstractClusterBuildingActionArgs buildingActionArgs;
  private final ActionAMSuicideArgs actionAMSuicideArgs = new ActionAMSuicideArgs();
  private final ActionBuildArgs actionBuildArgs = new ActionBuildArgs();
  private final ActionCreateArgs actionCreateArgs = new ActionCreateArgs();
  private final ActionDestroyArgs actionDestroyArgs = new ActionDestroyArgs();
  private final ActionExistsArgs actionExistsArgs = new ActionExistsArgs();
  private final ActionFlexArgs actionFlexArgs = new ActionFlexArgs();
  private final ActionForceKillArgs actionForceKillArgs =
    new ActionForceKillArgs();
  private final ActionFreezeArgs actionFreezeArgs = new ActionFreezeArgs();
  private final ActionGetConfArgs actionGetConfArgs = new ActionGetConfArgs();
  private final ActionKillContainerArgs actionKillContainerArgs =
    new ActionKillContainerArgs();
  private final ActionListArgs actionListArgs = new ActionListArgs();
  private final ActionStatusArgs actionStatusArgs = new ActionStatusArgs();
  private final ActionThawArgs actionThawArgs = new ActionThawArgs();
  private final ActionVersionArgs actionVersionArgs = new ActionVersionArgs();
  private final ActionHelpArgs actionHelpArgs = new ActionHelpArgs();


  public ClientArgs(String[] args) {
    super(args);
  }

  public ClientArgs(Collection<?> args) {
    super(args);
  }

  @Override
  protected void addActionArguments() {

    addActions(
      actionAMSuicideArgs,
      actionBuildArgs,
      actionCreateArgs,
      actionDestroyArgs,
      actionExistsArgs,
      actionFlexArgs,
      actionForceKillArgs,
      actionFreezeArgs,
      actionGetConfArgs,
      actionKillContainerArgs,
      actionListArgs,
      actionStatusArgs,
      actionThawArgs,
      actionHelpArgs,
      actionVersionArgs
              );
  }

  @Override
  public void applyDefinitions(Configuration conf) throws
                                                   BadCommandArgumentsException {
    super.applyDefinitions(conf);
    //RM
    if (getManager() != null) {
      log.debug("Setting RM to {}", getManager());
      conf.set(YarnConfiguration.RM_ADDRESS, getManager());
    }
    if ( getBasePath() != null ) {
      log.debug("Setting basePath to {}", getBasePath());
      conf.set(HoyaXmlConfKeys.KEY_BASE_HOYA_PATH, getBasePath().toString());
    }
  }

  public AbstractClusterBuildingActionArgs getBuildingActionArgs() {
    return buildingActionArgs;
  }

  public ActionAMSuicideArgs getActionAMSuicideArgs() {
    return actionAMSuicideArgs;
  }

  public ActionBuildArgs getActionBuildArgs() {
    return actionBuildArgs;
  }

  public ActionCreateArgs getActionCreateArgs() {
    return actionCreateArgs;
  }

  public ActionDestroyArgs getActionDestroyArgs() {
    return actionDestroyArgs;
  }

  public ActionExistsArgs getActionExistsArgs() {
    return actionExistsArgs;
  }

  public ActionFlexArgs getActionFlexArgs() {
    return actionFlexArgs;
  }

  public ActionForceKillArgs getActionForceKillArgs() {
    return actionForceKillArgs;
  }

  public ActionFreezeArgs getActionFreezeArgs() {
    return actionFreezeArgs;
  }

  public ActionGetConfArgs getActionGetConfArgs() {
    return actionGetConfArgs;
  }

  public ActionKillContainerArgs getActionKillContainerArgs() {
    return actionKillContainerArgs;
  }

  public ActionListArgs getActionListArgs() {
    return actionListArgs;
  }

  public ActionStatusArgs getActionStatusArgs() {
    return actionStatusArgs;
  }

  public ActionThawArgs getActionThawArgs() {
    return actionThawArgs;
  }

  /**
   * Look at the chosen action and bind it as the core action for the operation.
   * In theory this could be done by introspecting on the list of actions and 
   * choosing it without the switch statement. In practise this switch, while
   * verbose, is easier to debug.
   * @throws HoyaException bad argument or similar
   */
  @Override
  public void applyAction() throws HoyaException {
    String action = getAction();
    if (HoyaActions.ACTION_BUILD.equals(action)) {
      bindCoreAction(actionBuildArgs);
      //its a builder, so set those actions too
      buildingActionArgs = actionBuildArgs;
    } else if (HoyaActions.ACTION_CREATE.equals(action)) {
      bindCoreAction(actionCreateArgs);
      //its a builder, so set those actions too
      buildingActionArgs = actionCreateArgs;

    } else if (HoyaActions.ACTION_FREEZE.equals(action)) {
      bindCoreAction(actionFreezeArgs);

    } else if (HoyaActions.ACTION_THAW.equals(action)) {
      bindCoreAction(actionThawArgs);

    } else if (HoyaActions.ACTION_AM_SUICIDE.equals(action)) {
      bindCoreAction(actionAMSuicideArgs);

    } else if (HoyaActions.ACTION_DESTROY.equals(action)) {
      bindCoreAction(actionDestroyArgs);

    } else if (HoyaActions.ACTION_EMERGENCY_FORCE_KILL.equals(action)) {
      bindCoreAction(actionForceKillArgs);

    } else if (HoyaActions.ACTION_EXISTS.equals(action)) {
      bindCoreAction(actionExistsArgs);

    } else if (HoyaActions.ACTION_FLEX.equals(action)) {
      bindCoreAction(actionFlexArgs);

    } else if (HoyaActions.ACTION_GETCONF.equals(action)) {
      bindCoreAction(actionGetConfArgs);

    } else if (HoyaActions.ACTION_HELP.equals(action) ||
               HoyaActions.ACTION_USAGE.equals(action)) {
      bindCoreAction(actionHelpArgs);

    } else if (HoyaActions.ACTION_KILL_CONTAINER.equals(action)) {
      bindCoreAction(actionKillContainerArgs);

    } else if (HoyaActions.ACTION_LIST.equals(action)) {
      bindCoreAction(actionListArgs);

    } else if (HoyaActions.ACTION_STATUS.equals(action)) {
      bindCoreAction(actionStatusArgs);

    } else if (HoyaActions.ACTION_VERSION.equals(action)) {
      bindCoreAction(actionVersionArgs);

    } else if (action == null || action.isEmpty()) {
      throw new BadCommandArgumentsException(ErrorStrings.ERROR_NO_ACTION);

    } else {
      throw new BadCommandArgumentsException(ErrorStrings.ERROR_UNKNOWN_ACTION
                                             + " " + action);
    }
  }
}
