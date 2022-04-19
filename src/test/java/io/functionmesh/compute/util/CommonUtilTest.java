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

import static io.functionmesh.compute.util.CommonUtil.DEFAULT_FUNCTION_DOWNLOAD_DIRECTORY;
import static io.functionmesh.compute.util.CommonUtil.DEFAULT_FUNCTION_EXECUTABLE;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class CommonUtilTest {
    @Test
    public void testBuildDownloadPath() {
        // no data provided
        String downloadDirectory = "";
        String archivePath = "";
        String path = CommonUtil.buildDownloadPath(downloadDirectory, archivePath);
        assertEquals(DEFAULT_FUNCTION_DOWNLOAD_DIRECTORY + DEFAULT_FUNCTION_EXECUTABLE, path);

        // with downloadDirectory
        downloadDirectory = "/download";
        archivePath = "";
        path = CommonUtil.buildDownloadPath(downloadDirectory, archivePath);
        assertEquals(downloadDirectory + "/" + DEFAULT_FUNCTION_EXECUTABLE, path);

        // with downloadDirectory & archivePath
        downloadDirectory = "/download";
        archivePath = "a.jar";
        path = CommonUtil.buildDownloadPath(downloadDirectory, archivePath);
        assertEquals(downloadDirectory + "/" + archivePath, path);

        // with downloadDirectory & full archivePath
        downloadDirectory = "/download";
        archivePath = "/Users/sample/a.jar";
        path = CommonUtil.buildDownloadPath(downloadDirectory, archivePath);
        assertEquals(downloadDirectory + "/a.jar", path);

        // with full archivePath
        downloadDirectory = null;
        archivePath = "/Users/sample/a.jar";
        path = CommonUtil.buildDownloadPath(downloadDirectory, archivePath);
        assertEquals(DEFAULT_FUNCTION_DOWNLOAD_DIRECTORY + "a.jar", path);
    }
}
