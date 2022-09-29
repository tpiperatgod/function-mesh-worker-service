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
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;

/*
 This auth handler will read the auth parameter from the config and pass it to the function-mesh, which is not secure.
 Just used this for debug or test or compatible.
 */
@Slf4j
public class AuthHandlerInsecure implements AuthHandler {

    @Override
    public AuthResults handle(MeshWorkerService workerService, String clientRole,
                              AuthenticationDataSource authenticationDataSource,
                              String component) {
        AuthResults results = new AuthResults();
        if (!StringUtils.isEmpty(workerService.getWorkerConfig().getBrokerClientAuthenticationPlugin())
                && !StringUtils.isEmpty(
                workerService.getWorkerConfig().getBrokerClientAuthenticationParameters())) {
            Map<String, byte[]> valueMap = new HashMap<>();
            valueMap.put(CLIENT_AUTHENTICATION_PLUGIN_CLAIM,
                    workerService.getWorkerConfig().getBrokerClientAuthenticationPlugin().getBytes());
            byte[] finalParams = workerService.getWorkerConfig().getBrokerClientAuthenticationParameters().getBytes();
            try {
                ObjectMapper mapper = new ObjectMapper();
                OAuth2Parameters oauth2Parameters =
                        mapper.readValue(workerService.getWorkerConfig().getBrokerClientAuthenticationParameters(),
                                OAuth2Parameters.class);
                finalParams = mapper.writeValueAsBytes(oauth2Parameters);
            } catch (JsonProcessingException e) { // use the original parameters when exception happens
            }
            valueMap.put(CLIENT_AUTHENTICATION_PARAMETERS_CLAIM, finalParams);
            results.setAuthSecretData(valueMap);
        }
        return results;
    }

    @Override
    public void cleanUp(MeshWorkerService workerService, String clientRole, AuthenticationDataSource authDataHttps,
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
        } catch (IOException | ApiException e) {
            log.error("clean up auth for {}/{}/{} {} failed", tenant, namespace, componentName, e);
            throw new RuntimeException(e);
        }
    }
}
