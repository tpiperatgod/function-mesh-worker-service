/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.functionmesh.compute.auth;

import io.functionmesh.compute.MeshWorkerService;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;

public interface AuthHandler {
    String CLIENT_AUTHENTICATION_PLUGIN_CLAIM = "clientAuthenticationPlugin";
    String CLIENT_AUTHENTICATION_PARAMETERS_CLAIM = "clientAuthenticationParameters";

    AuthResults handle(MeshWorkerService workerService, String clientRole,
                       AuthenticationDataSource authDataHttps, String component);

    void cleanUp(MeshWorkerService workerService, String clientRole,
                 AuthenticationDataSource authDataHttps, String component,
                 String clusterName, String tenant, String namespace, String componentName);
}
