/*
 * Copyright 2012-2021 CodeLibs Project and the Others.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the Server Side Public License, version 1,
 * as published by MongoDB, Inc.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * Server Side Public License for more details.
 *
 * You should have received a copy of the Server Side Public License
 * along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package org.codelibs.elasticsearch.configsync;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.codelibs.curl.CurlResponse;
import org.codelibs.elasticsearch.configsync.service.ElasticsearchService;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.codelibs.elasticsearch.runner.net.EcrCurl;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class ConfigSyncApplicationTests {

    private static ElasticsearchClusterRunner runner;

    @Resource
    ElasticsearchService elasticsearchService;

    @BeforeAll
    static void beforeAll() {
        long now = System.currentTimeMillis();
        String clusterName = "es-configsync-" + now;
        // create runner instance
        runner = new ElasticsearchClusterRunner();
        // create ES nodes
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
                settingsBuilder.put("node.name", "es01");
                settingsBuilder.put("http.cors.enabled", true);
                settingsBuilder.put("http.cors.allow-origin", "*");
                settingsBuilder.putList("discovery.seed_hosts", "127.0.0.1:9301");
                settingsBuilder.putList("cluster.initial_master_nodes", "127.0.0.1");
            }
        }).build(newConfigs().clusterName(clusterName).numOfNode(1).useLogger().disableESLogger());

        // wait for yellow status
        runner.ensureYellow();

        Path appPath = Paths.get("target", "app-" + now);
        System.setProperty(ConfigSyncConstants.ELASTICSEARCH_HOME, appPath.toAbsolutePath().toString());
        System.setProperty(ConfigSyncConstants.ELASTICSEARCH_CLUSTER_NAME, clusterName);
    }

    @AfterAll
    static void afterAll() throws Exception {
        // close runner
        runner.close();
        // delete all files
        // runner.clean();
    }

    private void setupClusterRunnder(final boolean fileUpdaterEnabled, final String flushInterval) {
        final Path homePath = Paths.get(elasticsearchService.getRunner().getNode(0).settings().get("path.home"));
        elasticsearchService.destroy();
        try {
            runner.deleteIndex(".configsync");
        } catch (final Exception e) {
            // ignore
        }
        elasticsearchService.setOverwriteSettingsBuilder(builder -> {
            builder.put("configsync.flush_interval", flushInterval);
            builder.put("configsync.file_updater.enabled", fileUpdaterEnabled);
            builder.put("configsync.config_path", homePath.resolve("config").toAbsolutePath().toString());
        });
        elasticsearchService.initialize();

        for (int i = 0; i < 10; i++) {
            if (runner.indexExists(".configsync")) {
                break;
            }
            try {
                Thread.sleep(3000L);
            } catch (Exception e) {
                // nothing
            }
        }
    }

    @Test
    public void updaterDisabled() throws Exception {
        setupClusterRunnder(false, "1s");

        Node node = elasticsearchService.getRunner().node();

        File[] configFiles = new File[1];
        String homePath = elasticsearchService.getRunner().getNode(0).settings().get("path.home");
        configFiles[0] = new File(new File(homePath, "config"), "updaterDisabled1.txt");

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(0, list.size());
        }

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/file").header("Content-Type", "application/json")
                .param("path", configFiles[0].getName()).body("Test1").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(1, list.size());
            assertEquals(configFiles[0].getName(), list.get(0).toString());
        }

        Thread.sleep(3000L);

        assertFalse(configFiles[0].exists());

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/reset").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        Thread.sleep(3000L);

        assertTrue(configFiles[0].exists());
        assertEquals("Test1", new String(getText(configFiles[0])));
        configFiles[0].delete();
    }

    @Test
    public void configFiles() throws Exception {
        setupClusterRunnder(true, "1s");

        Node node = elasticsearchService.getRunner().node();

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/reset").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        File[] configFiles = new File[3];
        String homePath = elasticsearchService.getRunner().getNode(0).settings().get("path.home");
        File confPath = new File(homePath, "config");

        configFiles[0] = new File(confPath, "configFiles1.txt");
        configFiles[1] = new File(confPath, "dir1/configFiles2.txt");
        configFiles[2] = new File(confPath, "dir1/dir2/configFiles3.txt");

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(0, list.size());
        }

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/file").header("Content-Type", "application/json")
                .param("path", configFiles[0].getName()).body("Test1").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(1, list.size());
            assertEquals(configFiles[0].getName(), list.get(0).toString());
        }

        Thread.sleep(3000L);

        assertTrue(configFiles[0].exists());
        assertFalse(configFiles[1].exists());
        assertFalse(configFiles[2].exists());

        assertEquals("Test1", new String(getText(configFiles[0])));

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/file").header("Content-Type", "application/json")
                .body("{\"path\":\"dir1/" + configFiles[1].getName() + "\",\"content\":\""
                        + Base64.getEncoder().encodeToString("Test2".getBytes(StandardCharsets.UTF_8)) + "\"}")
                .execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(2, list.size());
            assertEquals(configFiles[0].getName(), list.get(0).toString());
            assertEquals("dir1/" + configFiles[1].getName(), list.get(1).toString());
        }

        try (CurlResponse response =
                EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").param("sort", "@timestamp").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(2, list.size());
            assertEquals(configFiles[0].getName(), list.get(0).toString());
            assertEquals("dir1/" + configFiles[1].getName(), list.get(1).toString());
        }

        Thread.sleep(3000L);

        assertTrue(configFiles[0].exists());
        assertTrue(configFiles[1].exists());
        assertFalse(configFiles[2].exists());

        assertEquals("Test1", new String(getText(configFiles[0])));
        assertEquals("Test2", new String(getText(configFiles[1])));

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/file").header("Content-Type", "application/json")
                .body("{\"path\":\"dir1/dir2/" + configFiles[2].getName() + "\",\"content\":\""
                        + Base64.getEncoder().encodeToString("Test3".getBytes(StandardCharsets.UTF_8)) + "\"}")
                .execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(3, list.size());
            assertEquals(configFiles[0].getName(), list.get(0).toString());
            assertEquals("dir1/" + configFiles[1].getName(), list.get(1).toString());
            assertEquals("dir1/dir2/" + configFiles[2].getName(), list.get(2).toString());
        }

        Thread.sleep(3000L);

        assertTrue(configFiles[0].exists(), () -> configFiles[0].getAbsolutePath());
        assertTrue(configFiles[1].exists(), () -> configFiles[1].getAbsolutePath());
        assertTrue(configFiles[2].exists(), () -> configFiles[2].getAbsolutePath());

        assertEquals("Test1", new String(getText(configFiles[0])));
        assertEquals("Test2", new String(getText(configFiles[1])));
        assertEquals("Test3", new String(getText(configFiles[2])));

        try (CurlResponse response = EcrCurl.delete(node, "/_configsync/file").header("Content-Type", "application/json")
                .param("path", "dir1/" + configFiles[1].getName()).execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
            assertEquals("deleted", contentMap.get("result").toString());
        }

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(2, list.size());
            assertEquals(configFiles[0].getName(), list.get(0).toString());
            assertEquals("dir1/dir2/" + configFiles[2].getName(), list.get(1).toString());
        }

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json")
                .param("fields", "path,@timestamp").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<Map<String, Object>> list = (List<Map<String, Object>>) contentMap.get("file");
            assertEquals(2, list.size());
            assertEquals(configFiles[0].getName(), list.get(0).get("path"));
            assertTrue(list.get(0).get("@timestamp").toString().startsWith("20"));
            assertEquals("dir1/dir2/" + configFiles[2].getName(), list.get(1).get("path"));
            assertTrue(list.get(1).get("@timestamp").toString().startsWith("20"));
        }

        try (CurlResponse response = EcrCurl.delete(node, "/_configsync/file").header("Content-Type", "application/json")
                .body("{\"path\":\"" + configFiles[0].getName() + "\"}").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
            assertEquals("deleted", contentMap.get("result").toString());
        }

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(1, list.size());
            assertEquals("dir1/dir2/" + configFiles[2].getName(), list.get(0).toString());
        }

        try (CurlResponse response = EcrCurl.delete(node, "/_configsync/file").header("Content-Type", "application/json")
                .body("{\"path\":\"" + configFiles[2].getName() + "\"}").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
            assertEquals("not_found", contentMap.get("result").toString());
        }
    }

    @Test
    public void configFilesWithFlush() throws Exception {
        setupClusterRunnder(true, "1m");

        Node node = elasticsearchService.getRunner().node();

        File[] configFiles = new File[3];
        String homePath = elasticsearchService.getRunner().getNode(0).settings().get("path.home");
        File confPath = new File(homePath, "config");

        configFiles[0] = new File(confPath, "configFilesWithFlush1.txt");
        configFiles[1] = new File(confPath, "dir1/configFilesWithFlush2.txt");
        configFiles[2] = new File(confPath, "dir1/dir2/configFilesWithFlush3.txt");

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(0, list.size());
        }

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/file").header("Content-Type", "application/json")
                .param("path", configFiles[0].getName()).body("Test1").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(1, list.size());
            assertEquals(configFiles[0].getName(), list.get(0).toString());
        }

        assertFalse(configFiles[0].exists());
        assertFalse(configFiles[1].exists());
        assertFalse(configFiles[2].exists());

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/flush").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        assertTrue(configFiles[0].exists());
        assertFalse(configFiles[1].exists());
        assertFalse(configFiles[2].exists());

        assertEquals("Test1", new String(getText(configFiles[0])));

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/file").header("Content-Type", "application/json")
                .body("{\"path\":\"dir1/" + configFiles[1].getName() + "\",\"content\":\""
                        + Base64.getEncoder().encodeToString("Test2".getBytes(StandardCharsets.UTF_8)) + "\"}")
                .execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        try (CurlResponse response =
                EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").param("sort", "@timestamp").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(2, list.size());
            assertEquals(configFiles[0].getName(), list.get(0).toString());
            assertEquals("dir1/" + configFiles[1].getName(), list.get(1).toString());
        }

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/flush").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        assertTrue(configFiles[0].exists());
        assertTrue(configFiles[1].exists());
        assertFalse(configFiles[2].exists());

        assertEquals("Test1", new String(getText(configFiles[0])));
        assertEquals("Test2", new String(getText(configFiles[1])));

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/file").header("Content-Type", "application/json")
                .body("{\"path\":\"dir1/dir2/" + configFiles[2].getName() + "\",\"content\":\""
                        + Base64.getEncoder().encodeToString("Test3".getBytes(StandardCharsets.UTF_8)) + "\"}")
                .execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        try (CurlResponse response =
                EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").param("sort", "@timestamp").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(3, list.size());
            assertEquals(configFiles[0].getName(), list.get(0).toString());
            assertEquals("dir1/" + configFiles[1].getName(), list.get(1).toString());
            assertEquals("dir1/dir2/" + configFiles[2].getName(), list.get(2).toString());
        }

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/flush").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        assertTrue(configFiles[0].exists());
        assertTrue(configFiles[1].exists());
        assertTrue(configFiles[2].exists());

        assertEquals("Test1", new String(getText(configFiles[0])));
        assertEquals("Test2", new String(getText(configFiles[1])));
        assertEquals("Test3", new String(getText(configFiles[2])));

        try (CurlResponse response = EcrCurl.delete(node, "/_configsync/file").header("Content-Type", "application/json")
                .param("path", "dir1/" + configFiles[1].getName()).execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
            assertEquals("deleted", contentMap.get("result").toString());
        }

        try (CurlResponse response =
                EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").param("sort", "@timestamp").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(2, list.size());
            assertEquals(configFiles[0].getName(), list.get(0).toString());
            assertEquals("dir1/dir2/" + configFiles[2].getName(), list.get(1).toString());
        }

        try (CurlResponse response = EcrCurl.delete(node, "/_configsync/file").header("Content-Type", "application/json")
                .body("{\"path\":\"" + configFiles[0].getName() + "\"}").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
            assertEquals("deleted", contentMap.get("result").toString());
        }

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(1, list.size());
            assertEquals("dir1/dir2/" + configFiles[2].getName(), list.get(0).toString());
        }

        try (CurlResponse response = EcrCurl.delete(node, "/_configsync/file").header("Content-Type", "application/json")
                .body("{\"path\":\"" + configFiles[2].getName() + " \"}").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser());
            assertEquals("true", contentMap.get("acknowledged").toString());
            assertEquals("not_found", contentMap.get("result").toString());
        }
    }

    private static byte[] getText(File file) throws IOException {
        byte[] buffer = new byte[1000];
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
                ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            int n = 0;
            while ((n = bis.read(buffer)) >= 0) {
                baos.write(buffer, 0, n);
            }
            return baos.toByteArray();
        }
    }

}
