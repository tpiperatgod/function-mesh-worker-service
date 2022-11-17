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
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPulsarAuthConfig;
import io.functionmesh.compute.functions.models.V1alpha1FunctionSpecPulsarAuthConfigOauth2Config;
import io.functionmesh.compute.models.OAuth2Parameters;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPulsarAuthConfig;
import io.functionmesh.compute.sinks.models.V1alpha1SinkSpecPulsarAuthConfigOauth2Config;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecPulsarAuthConfig;
import io.functionmesh.compute.sources.models.V1alpha1SourceSpecPulsarAuthConfigOauth2Config;
import io.functionmesh.compute.util.CommonUtil;
import io.kubernetes.client.openapi.models.V1SecretList;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;

@Slf4j
public class AuthHandlerOauth implements AuthHandler {
    // the provided secret should use this to store the oauth2 parameters
    public static final String KEY_NAME = "auth.json";

    @Override
    public AuthResults handle(MeshWorkerService workerService, String clientRole,
                              AuthenticationDataHttps authDataHttps, String component) {
        if (StringUtils.isNotEmpty(clientRole)) {
            String secretName;
            try {
                String annotationKey = workerService.getMeshWorkerServiceCustomConfig().getOauth2SecretAnnotationKey();
                log.info("get secret from namespace: {}, using annotation key to filter: {}, clientRole is: {}",
                        workerService.getJobNamespace(), annotationKey, clientRole);
                V1SecretList secrets =
                        workerService.getCoreV1Api().listNamespacedSecret(workerService.getJobNamespace(), null, null,
                                null, null, null, null, null, null, null, null);
                secretName = secrets.getItems().stream().filter(secret -> {
                    if (secret.getMetadata() != null && secret.getMetadata().getAnnotations() != null) {
                        return clientRole.equals(secret.getMetadata().getAnnotations().get(annotationKey));
                    }
                    return false;
                }).findFirst().get().getMetadata().getName();
            } catch (Exception e) {
                throw new RuntimeException("Failed to get oauth2 private key secret", e);
            }
            try {
                ObjectMapper mapper = new ObjectMapper();
                OAuth2Parameters oauth2Parameters =
                        mapper.readValue(
                                workerService.getWorkerConfig().getBrokerClientAuthenticationParameters(),
                                OAuth2Parameters.class);
                AuthResults results = new AuthResults();

                UpdateOAuth2Fields(component, oauth2Parameters, secretName, KEY_NAME, results);
                return results;
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to parse oauth2 parameters", e);
            }
        }
        throw new RuntimeException("Client role is empty");
    }

    @Override
    public void cleanUp(MeshWorkerService workerService, String clientRole, AuthenticationDataHttps authDataHttps,
                        String component, String clusterName, String tenant, String namespace, String componentName) {
        // Do nothing for oauth2 handler
    }

    public static void UpdateOAuth2Fields(String component, OAuth2Parameters oauth2Parameters, String secretName,
                                          String secretKey, AuthResults results) {
        switch (component.toLowerCase()) {
            case CommonUtil.COMPONENT_FUNCTION:
                V1alpha1FunctionSpecPulsarAuthConfigOauth2Config oauth2 =
                        new V1alpha1FunctionSpecPulsarAuthConfigOauth2Config()
                                .audience(oauth2Parameters.getAudience())
                                .issuerUrl(oauth2Parameters.getIssuerUrl())
                                .keySecretName(secretName)
                                .keySecretKey(secretKey);
                if (StringUtils.isNotEmpty(oauth2Parameters.getScope())) {
                    oauth2.setScope(oauth2Parameters.getScope());
                }
                results.setFunctionAuthConfig(new V1alpha1FunctionSpecPulsarAuthConfig().oauth2Config(oauth2));
                break;
            case CommonUtil.COMPONENT_SINK:
                V1alpha1SinkSpecPulsarAuthConfigOauth2Config sinkOAuth2 =
                        new V1alpha1SinkSpecPulsarAuthConfigOauth2Config()
                                .audience(oauth2Parameters.getAudience())
                                .issuerUrl(oauth2Parameters.getIssuerUrl())
                                .keySecretName(secretName)
                                .keySecretKey(KEY_NAME);
                if (StringUtils.isNotEmpty(oauth2Parameters.getScope())) {
                    sinkOAuth2.setScope(oauth2Parameters.getScope());
                }
                results.setSinkAuthConfig(new V1alpha1SinkSpecPulsarAuthConfig().oauth2Config(sinkOAuth2));
                break;
            case CommonUtil.COMPONENT_SOURCE:
                V1alpha1SourceSpecPulsarAuthConfigOauth2Config sourceOAuth2 =
                        new V1alpha1SourceSpecPulsarAuthConfigOauth2Config()
                                .audience(oauth2Parameters.getAudience())
                                .issuerUrl(oauth2Parameters.getIssuerUrl())
                                .keySecretName(secretName)
                                .keySecretKey(KEY_NAME);
                if (StringUtils.isNotEmpty(oauth2Parameters.getScope())) {
                    sourceOAuth2.setScope(oauth2Parameters.getScope());
                }
                results.setSourceAuthConfig(new V1alpha1SourceSpecPulsarAuthConfig().oauth2Config(sourceOAuth2));
                break;
            default:
                break;
        }
    }
}
