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
package io.functionmesh.compute.util;

import static io.functionmesh.compute.models.PackageMetadataProperties.PROPERTY_CHECKSUM;
import static io.functionmesh.compute.models.PackageMetadataProperties.PROPERTY_FILE_NAME;
import static io.functionmesh.compute.models.PackageMetadataProperties.PROPERTY_FILE_SIZE;
import static io.functionmesh.compute.models.PackageMetadataProperties.PROPERTY_FUNCTION_NAME;
import static io.functionmesh.compute.models.PackageMetadataProperties.PROPERTY_MANAGED_BY_MESH_WORKER_SERVICE;
import static io.functionmesh.compute.models.PackageMetadataProperties.PROPERTY_NAMESPACE;
import static io.functionmesh.compute.models.PackageMetadataProperties.PROPERTY_TENANT;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.packages.management.core.common.PackageMetadata;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

@Slf4j
public class PackageManagementServiceUtil {

    public final static String MESH_WORKER_SERVICE_PACKAGE_CONTACT = "mesh-worker-service";
    public final static String PACKAGE_TYPE_FUNCTION = "function";
    public final static String PACKAGE_TYPE_SINK = "sink";
    public final static String PACKAGE_TYPE_SOURCE = "source";

    private static String generatePackageURL(final String type,
                                             final String tenant,
                                             final String namespace,
                                             final String functionName) {
        return String.format("%s://%s/%s/%s", type, tenant, namespace, functionName);
    }

    public static String uploadPackageToPackageService(PulsarAdmin admin,
                                                       final String type,
                                                       final String tenant,
                                                       final String namespace,
                                                       final String functionName,
                                                       final InputStream uploadedInputStream,
                                                       final FormDataContentDisposition fileDetail,
                                                       String tempDirectory) throws Exception {
        Path tempDirectoryPath = Paths.get(tempDirectory);
        if (Files.notExists(tempDirectoryPath)) {
            Files.createDirectories(tempDirectoryPath);
        }
        Path filePath = Files.createTempFile(tempDirectoryPath,
                RandomStringUtils.random(5, true, true).toLowerCase(), fileDetail.getFileName());
        FileUtils.copyInputStreamToFile(uploadedInputStream, filePath.toFile());
        uploadedInputStream.close();

        String packageName = generatePackageURL(type, tenant, namespace, functionName);
        try {
            log.info("Try to overwrite the function file if it is already exists at '{}'.", packageName);
            deletePackageFromPackageService(admin, type, tenant, namespace, functionName);
        } catch (Exception ex) {
            log.warn("Overwriting function package '{}' failed", packageName, ex);
        }
        PackageMetadata packageMetadata = new PackageMetadata();
        packageMetadata.setContact(MESH_WORKER_SERVICE_PACKAGE_CONTACT);
        packageMetadata.setDescription("mesh-worker-service created for " + packageName);
        Map<String, String> properties = new HashMap<>();
        properties.put(PROPERTY_TENANT, tenant);
        properties.put(PROPERTY_NAMESPACE, namespace);
        properties.put(PROPERTY_FUNCTION_NAME, functionName);
        properties.put(PROPERTY_FILE_NAME, fileDetail.getFileName());
        properties.put(PROPERTY_FILE_SIZE, Long.toString(filePath.toFile().length()));
        long checksum = FileUtils.checksumCRC32(filePath.toFile());
        properties.put(PROPERTY_CHECKSUM, Long.toString(checksum));
        properties.put(PROPERTY_MANAGED_BY_MESH_WORKER_SERVICE, String.valueOf(true));
        packageMetadata.setProperties(properties);
        admin.packages().upload(packageMetadata, packageName, filePath.toString());
        log.info("upload file {} to package service {} successfully", filePath, packageName);
        Files.deleteIfExists(filePath);
        return packageName;
    }

    public static void deletePackageFromPackageService(PulsarAdmin admin,
                                                       final String type,
                                                       final String tenant,
                                                       final String namespace,
                                                       final String functionName) throws Exception {
        String packageName = generatePackageURL(type, tenant, namespace, functionName);
        try {
            PackageMetadata packageMetadata = admin.packages().getMetadata(packageName);
            if (packageMetadata != null && packageMetadata.getProperties().containsKey(PROPERTY_FILE_NAME) &&
                    StringUtils.isNotEmpty(packageMetadata.getProperties().get(PROPERTY_FILE_NAME)) &&
                    StringUtils.isNotEmpty(packageMetadata.getContact()) &&
                    packageMetadata.getContact().equals(MESH_WORKER_SERVICE_PACKAGE_CONTACT)) {
                admin.packages().delete(packageName);
            }
        } catch (PulsarAdminException.NotFoundException ignore) {}
    }

    public static String getPackageTypeFromComponentType(Function.FunctionDetails.ComponentType componentType) {
        switch (componentType) {
            case FUNCTION:
                return PACKAGE_TYPE_FUNCTION;
            case SINK:
                return PACKAGE_TYPE_SINK;
            case SOURCE:
                return PACKAGE_TYPE_SOURCE;
            default:
                return "";
        }
    }
}
