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

import static io.functionmesh.compute.util.CommonUtil.OAUTH_PLUGIN_NAME;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.functionmesh.compute.MeshWorkerService;
import io.functionmesh.compute.models.OAuth2Parameters;
import io.functionmesh.compute.util.KubernetesUtils;
import io.kubernetes.client.openapi.ApiException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;

/*
 This auth handler will read the auth parameter from the config and pass it to the function-mesh, which is not secure.
 Just used this for debug or test or compatible.
 */
@Slf4j
public class AuthHandlerInsecure implements AuthHandler {

    @Override
    public AuthResults handle(MeshWorkerService workerService, String clientRole, AuthenticationDataHttps authDataHttps,
                              String component) {
        AuthResults results = new AuthResults();
        if (!StringUtils.isEmpty(workerService.getWorkerConfig().getBrokerClientAuthenticationPlugin())
                && !StringUtils.isEmpty(
                workerService.getWorkerConfig().getBrokerClientAuthenticationParameters())) {
            switch (workerService.getWorkerConfig().getBrokerClientAuthenticationPlugin()) {
                case OAUTH_PLUGIN_NAME:
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        OAuth2Parameters oauth2Parameters =
                                mapper.readValue(
                                        workerService.getWorkerConfig().getBrokerClientAuthenticationParameters(),
                                        OAuth2Parameters.class);
                        String[] paths = oauth2Parameters.getPrivateKey().split("/");
                        if (paths.length == 0 || StringUtils.isEmpty(
                                workerService.getMeshWorkerServiceCustomConfig().getOauth2SecretName())) {
                            throw new RuntimeException("privateKey is empty or oauth2SecretName is not set");
                        }
                        String secretKey = paths[paths.length - 1];
                        AuthHandlerOauth.UpdateOAuth2Fields(component, oauth2Parameters,
                                workerService.getMeshWorkerServiceCustomConfig().getOauth2SecretName(), secretKey,
                                results);
                    } catch (RuntimeException | JsonProcessingException e) { // fallback to auth secret way
                        log.error("failed to read oauth2 parameters {}", e.getMessage());
                        Map<String, byte[]> valueMap = new HashMap<>();
                        valueMap.put(CLIENT_AUTHENTICATION_PLUGIN_CLAIM,
                                workerService.getWorkerConfig().getBrokerClientAuthenticationPlugin().getBytes());
                        valueMap.put(CLIENT_AUTHENTICATION_PARAMETERS_CLAIM,
                                workerService.getWorkerConfig().getBrokerClientAuthenticationParameters().getBytes());
                        results.setAuthSecretData(valueMap);
                    }
                    break;
                default:
                    Map<String, byte[]> valueMap = new HashMap<>();
                    valueMap.put(CLIENT_AUTHENTICATION_PLUGIN_CLAIM,
                            workerService.getWorkerConfig().getBrokerClientAuthenticationPlugin().getBytes());
                    valueMap.put(CLIENT_AUTHENTICATION_PARAMETERS_CLAIM,
                            workerService.getWorkerConfig().getBrokerClientAuthenticationParameters().getBytes());
                    results.setAuthSecretData(valueMap);
                    break;
            }
        }
        return results;
    }

    @Override
    public void cleanUp(MeshWorkerService workerService, String clientRole, AuthenticationDataHttps authDataHttps,
                        String component, String clusterName, String tenant, String namespace, String componentName) {

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
            log.error("clean up auth for {}/{}/{} {} failed", tenant, namespace, componentName, e.getMessage());
            // do nothing if auth secret doesn't exist
            if (e.getCode() != 404) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            log.error("clean up auth for {}/{}/{} {} failed", tenant, namespace, componentName, e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
