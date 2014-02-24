/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hoya.yarn.appmaster.web;

import org.apache.hoya.api.HoyaClusterProtocol;
import org.apache.hoya.providers.ProviderService;
import org.apache.hoya.yarn.appmaster.state.AppState;

/**
 * 
 */
public class WebAppApiImpl implements WebAppApi {

  protected final HoyaClusterProtocol clusterProto;
  protected final AppState appState;
  protected final ProviderService provider;
  
  public WebAppApiImpl(HoyaClusterProtocol clusterProto, AppState appState, ProviderService provider) {
    this.clusterProto = clusterProto;
    this.appState = appState;
    this.provider = provider;
  }
  
  public AppState getAppState() {
    return appState;
  }
  
  public ProviderService getProviderService() {
    return provider;
  }
  
  public HoyaClusterProtocol getClusterProtocol() {
    return clusterProto;
  }
  
}
