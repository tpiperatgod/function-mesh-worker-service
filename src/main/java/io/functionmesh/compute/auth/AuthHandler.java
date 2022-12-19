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
import io.functionmesh.compute.util.KubernetesUtils;
import io.kubernetes.client.openapi.ApiException;
import java.io.IOException;
import okhttp3.Call;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;

public interface AuthHandler {
    String CLIENT_AUTHENTICATION_PLUGIN_CLAIM = "clientAuthenticationPlugin";
    String CLIENT_AUTHENTICATION_PARAMETERS_CLAIM = "clientAuthenticationParameters";

    AuthResults handle(MeshWorkerService workerService, String clientRole,
                       AuthenticationDataHttps authDataHttps, String component);

    default void cleanUp(MeshWorkerService workerService, String clientRole,
                 AuthenticationDataHttps authDataHttps, String component,
                 String clusterName, String tenant, String namespace, String componentName) {

        try {
            Call deleteAuthSecretCall = workerService.getCoreV1Api()
                    .deleteNamespacedSecretCall(
                            KubernetesUtils.getUniqueSecretName(
                                    component.toLowerCase(),
                                    "auth",
                                    DigestUtils.sha256Hex(
                                            KubernetesUtils.getSecretName(
                                                    clusterName, tenant, namespace, componentName))),
                            workerService.getJobNamespace(),
                            null,
                            null,
                            30,
                            false,
                            null,
                            null,
                            null
                    );
            deleteAuthSecretCall.execute();
        } catch (ApiException e) {
            // do nothing if auth secret doesn't exist
            if (e.getCode() != 404) {
                throw new RuntimeException(e.getMessage());
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
