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
package io.functionmesh.compute.worker;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.functionmesh.compute.models.FunctionMeshConnectorDefinition;
import io.functionmesh.compute.models.MeshWorkerServiceCustomConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.util.ObjectMapperFactory;

@Slf4j
public class MeshConnectorsManager {
    private final Path connectorDefinitionsFilePath;
    private final long connectorSearchIntervalSeconds;
    private final ScheduledExecutorService searchExecutor =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                    .setNameFormat("function-mesh-worker-service-connector-search-%d")
                    .build());
    private long connectorDefinitionsLastModifiedMs = 0;

    @Getter
    private volatile TreeMap<String, FunctionMeshConnectorDefinition> connectors = new TreeMap<>();

    public MeshConnectorsManager(MeshWorkerServiceCustomConfig customConfig) {
        this.connectorDefinitionsFilePath = Paths.get(customConfig.getConnectorDefinitionsFilePath()).toAbsolutePath();
        this.connectorSearchIntervalSeconds = customConfig.getConnectorSearchIntervalSeconds();

        this.searchExecutor.scheduleWithFixedDelay(this::loadConnectorsIfNeeded, 0, connectorSearchIntervalSeconds, TimeUnit.SECONDS);
    }

    private void loadConnectorsIfNeeded() {
        try {
            long lastModifiedMs = Files.getLastModifiedTime(connectorDefinitionsFilePath).toMillis(); // Symbolic links are followed
            if (lastModifiedMs > connectorDefinitionsLastModifiedMs) {
                this.connectors = loadConnectorDefinitionsFromFile(connectorDefinitionsFilePath);
                this.connectorDefinitionsLastModifiedMs = lastModifiedMs;
            }
        } catch (IOException e) {
            log.error("Cannot load connector definitions", e);
        }
    }

    static TreeMap<String, FunctionMeshConnectorDefinition> loadConnectorDefinitionsFromFile(Path path)
            throws IOException {
        log.info("Loading connector definitions from {}", path);

        TreeMap<String, FunctionMeshConnectorDefinition> results = new TreeMap<>();

        if (!path.toFile().exists()) {
            log.error("Connector definitions file {} not found", path);
            return results;
        }

        String configs = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        FunctionMeshConnectorDefinition[] data = ObjectMapperFactory.getThreadLocalYaml()
                .readValue(configs, FunctionMeshConnectorDefinition[].class);
        for (FunctionMeshConnectorDefinition d : data) {
            results.put(d.getName(), d);
        }

        return results;
    }

    public FunctionMeshConnectorDefinition getConnectorDefinition(String connectorType) {
        return connectors.get(connectorType);
    }

    public void reloadConnectors() {
        searchExecutor.submit(this::loadConnectorsIfNeeded);
    }

    public List<ConnectorDefinition> getConnectorDefinitions() {
        return connectors.values().stream().map(v -> (ConnectorDefinition) v).collect(Collectors.toList());
    }
}
