package org.codelibs.elasticsearch.configsync;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.codelibs.curl.CurlResponse;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.codelibs.elasticsearch.runner.net.EcrCurl;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;

import junit.framework.TestCase;

public class ConfigSyncPluginTest extends TestCase {

    private ElasticsearchClusterRunner runner;

    private int numOfNode = 3;

    private File[] configFiles;

    private String clusterName;

    private void setupClusterRunnder(final Boolean fileUpdaterEnabled, final String flushInterval) {
        clusterName = "es-configsync-" + System.currentTimeMillis();
        // create runner instance
        runner = new ElasticsearchClusterRunner();
        // create ES nodes
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
                settingsBuilder.put("http.cors.enabled", true);
                settingsBuilder.put("http.cors.allow-origin", "*");
                settingsBuilder.putList("discovery.seed_hosts", "127.0.0.1:9301");
                settingsBuilder.putList("cluster.initial_master_nodes", "127.0.0.1:9301");
                settingsBuilder.put("configsync.flush_interval", flushInterval);
                if (fileUpdaterEnabled != null) {
                    settingsBuilder.put("configsync.file_updater.enabled", fileUpdaterEnabled.booleanValue());
                }
            }
        }).build(newConfigs().clusterName(clusterName).numOfNode(numOfNode)
                .pluginTypes("org.codelibs.elasticsearch.configsync.ConfigSyncPlugin"));

        // wait for yellow status
        runner.ensureYellow();

        configFiles = null;

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

    @Override
    protected void tearDown() throws Exception {
        // close runner
        runner.close();
        // delete all files
        runner.clean();
        if (configFiles != null) {
            for (File file : configFiles) {
                file.deleteOnExit();
            }
        }
    }

    public void test_updaterDisabled() throws Exception {
        setupClusterRunnder(false, "1s");

        Node node = runner.node();

        configFiles = new File[numOfNode];
        for (int i = 0; i < numOfNode; i++) {
            String homePath = runner.getNode(i).settings().get("path.home");
            configFiles[i] = new File(new File(homePath, "config"), "test1.txt");
        }

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(0, list.size());
        }

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/file").header("Content-Type", "application/json")
                .param("path", "test1.txt").body("Test1").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(1, list.size());
            assertEquals("test1.txt", list.get(0).toString());
        }

        Thread.sleep(3000L);

        for (int i = 0; i < numOfNode; i++) {
            assertFalse(configFiles[i].exists());
        }

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/reset").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        Thread.sleep(3000L);

        for (int i = 0; i < numOfNode; i++) {
            assertTrue(configFiles[i].exists());
            assertEquals("Test1", new String(getText(configFiles[i])));
        }
    }

    public void test_configFiles() throws Exception {
        setupClusterRunnder(null, "1m");

        Node node = runner.node();

        {
            Settings settings = Settings.builder().put("configsync.flush_interval", "1s").build();
            ClusterUpdateSettingsResponse response =
                    node.client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).execute().actionGet();
            assertTrue(response.isAcknowledged());
        }

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/reset").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        configFiles = new File[numOfNode * 3];
        for (int i = 0; i < numOfNode; i++) {
            String homePath = runner.getNode(i).settings().get("path.home");
            File confPath = new File(homePath, "config");

            int base = i * 3;
            configFiles[base] = new File(confPath, "test1.txt");
            configFiles[base + 1] = new File(confPath, "dir1/test2.txt");
            configFiles[base + 2] = new File(confPath, "dir1/dir2/test3.txt");
        }

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(0, list.size());
        }

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/file").header("Content-Type", "application/json")
                .param("path", "test1.txt").body("Test1").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(1, list.size());
            assertEquals("test1.txt", list.get(0).toString());
        }

        Thread.sleep(3000L);

        for (int i = 0; i < numOfNode; i++) {
            int base = i * 3;
            assertTrue(configFiles[base].exists());
            assertFalse(configFiles[base + 1].exists());
            assertFalse(configFiles[base + 2].exists());

            assertEquals("Test1", new String(getText(configFiles[base])));
        }

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/file").header("Content-Type", "application/json")
                .body("{\"path\":\"dir1/test2.txt\",\"content\":\""
                        + Base64.getEncoder().encodeToString("Test2".getBytes(StandardCharsets.UTF_8)) + "\"}")
                .execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(2, list.size());
            assertEquals("dir1/test2.txt", list.get(0).toString());
            assertEquals("test1.txt", list.get(1).toString());
        }

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").param("sort", "@timestamp").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(2, list.size());
            assertEquals("test1.txt", list.get(0).toString());
            assertEquals("dir1/test2.txt", list.get(1).toString());
        }

        Thread.sleep(3000L);

        for (int i = 0; i < numOfNode; i++) {
            int base = i * 3;
            assertTrue(configFiles[base].exists());
            assertTrue(configFiles[base + 1].exists());
            assertFalse(configFiles[base + 2].exists());

            assertEquals("Test1", new String(getText(configFiles[base])));
            assertEquals("Test2", new String(getText(configFiles[base + 1])));
        }

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/file").header("Content-Type", "application/json")
                .body("{\"path\":\"dir1/dir2/test3.txt\",\"content\":\""
                        + Base64.getEncoder().encodeToString("Test3".getBytes(StandardCharsets.UTF_8)) + "\"}")
                .execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(3, list.size());
            assertEquals("dir1/dir2/test3.txt", list.get(0).toString());
            assertEquals("dir1/test2.txt", list.get(1).toString());
            assertEquals("test1.txt", list.get(2).toString());
        }

        Thread.sleep(3000L);

        for (int i = 0; i < numOfNode; i++) {
            int base = i * 3;
            assertTrue(configFiles[base].getAbsolutePath(), configFiles[base].exists());
            assertTrue(configFiles[base + 1].getAbsolutePath(), configFiles[base + 1].exists());
            assertTrue(configFiles[base + 2].getAbsolutePath(), configFiles[base + 2].exists());

            assertEquals("Test1", new String(getText(configFiles[base])));
            assertEquals("Test2", new String(getText(configFiles[base + 1])));
            assertEquals("Test3", new String(getText(configFiles[base + 2])));
        }

        try (CurlResponse response = EcrCurl.delete(node, "/_configsync/file").header("Content-Type", "application/json")
                .param("path", "dir1/test2.txt").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
            assertEquals("deleted", contentMap.get("result").toString());
        }

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(2, list.size());
            assertEquals("dir1/dir2/test3.txt", list.get(0).toString());
            assertEquals("test1.txt", list.get(1).toString());
        }

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json")
                .param("fields", "path,@timestamp").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<Map<String, Object>> list = (List<Map<String, Object>>) contentMap.get("file");
            assertEquals(2, list.size());
            assertEquals("dir1/dir2/test3.txt", list.get(0).get("path"));
            assertTrue(list.get(0).get("@timestamp").toString().startsWith("20"));
            assertEquals("test1.txt", list.get(1).get("path"));
            assertTrue(list.get(1).get("@timestamp").toString().startsWith("20"));
        }

        try (CurlResponse response = EcrCurl.delete(node, "/_configsync/file").header("Content-Type", "application/json")
                .body("{\"path\":\"test1.txt\"}").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
            assertEquals("deleted", contentMap.get("result").toString());
        }

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(1, list.size());
            assertEquals("dir1/dir2/test3.txt", list.get(0).toString());
        }

        try (CurlResponse response = EcrCurl.delete(node, "/_configsync/file").header("Content-Type", "application/json").body("{\"path\":\"test3.txt\"}").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
            assertEquals("not_found", contentMap.get("result").toString());
        }
    }

    public void test_configFiles_withFlush() throws Exception {
        setupClusterRunnder(null, "1m");

        Node node = runner.node();

        configFiles = new File[numOfNode * 3];
        for (int i = 0; i < numOfNode; i++) {
            String homePath = runner.getNode(i).settings().get("path.home");
            File confPath = new File(homePath, "config");

            int base = i * 3;
            configFiles[base] = new File(confPath, "test1.txt");
            configFiles[base + 1] = new File(confPath, "dir1/test2.txt");
            configFiles[base + 2] = new File(confPath, "dir1/dir2/test3.txt");
        }

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(0, list.size());
        }

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/file").header("Content-Type", "application/json").param("path", "test1.txt").body("Test1").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(1, list.size());
            assertEquals("test1.txt", list.get(0).toString());
        }

        for (int i = 0; i < numOfNode; i++) {
            int base = i * 3;
            assertFalse(configFiles[base].exists());
            assertFalse(configFiles[base + 1].exists());
            assertFalse(configFiles[base + 2].exists());
        }

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/flush").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        for (int i = 0; i < numOfNode; i++) {
            int base = i * 3;
            assertTrue(configFiles[base].exists());
            assertFalse(configFiles[base + 1].exists());
            assertFalse(configFiles[base + 2].exists());

            assertEquals("Test1", new String(getText(configFiles[base])));
        }

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/file").header("Content-Type", "application/json")
                .body("{\"path\":\"dir1/test2.txt\",\"content\":\""
                        + Base64.getEncoder().encodeToString("Test2".getBytes(StandardCharsets.UTF_8)) + "\"}")
                .execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        try (CurlResponse response =
                EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").param("sort", "@timestamp").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(2, list.size());
            assertEquals("test1.txt", list.get(0).toString());
            assertEquals("dir1/test2.txt", list.get(1).toString());
        }

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/flush").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        for (int i = 0; i < numOfNode; i++) {
            int base = i * 3;
            assertTrue(configFiles[base].exists());
            assertTrue(configFiles[base + 1].exists());
            assertFalse(configFiles[base + 2].exists());

            assertEquals("Test1", new String(getText(configFiles[base])));
            assertEquals("Test2", new String(getText(configFiles[base + 1])));
        }

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/file").header("Content-Type", "application/json")
                .body("{\"path\":\"dir1/dir2/test3.txt\",\"content\":\""
                        + Base64.getEncoder().encodeToString("Test3".getBytes(StandardCharsets.UTF_8)) + "\"}")
                .execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        try (CurlResponse response =
                EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").param("sort", "@timestamp").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(3, list.size());
            assertEquals("test1.txt", list.get(0).toString());
            assertEquals("dir1/test2.txt", list.get(1).toString());
            assertEquals("dir1/dir2/test3.txt", list.get(2).toString());
        }

        try (CurlResponse response = EcrCurl.post(node, "/_configsync/flush").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        for (int i = 0; i < numOfNode; i++) {
            int base = i * 3;
            assertTrue(configFiles[base].exists());
            assertTrue(configFiles[base + 1].exists());
            assertTrue(configFiles[base + 2].exists());

            assertEquals("Test1", new String(getText(configFiles[base])));
            assertEquals("Test2", new String(getText(configFiles[base + 1])));
            assertEquals("Test3", new String(getText(configFiles[base + 2])));
        }

        try (CurlResponse response = EcrCurl.delete(node, "/_configsync/file").header("Content-Type", "application/json")
                .param("path", "dir1/test2.txt").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
            assertEquals("deleted", contentMap.get("result").toString());
        }

        try (CurlResponse response =
                EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").param("sort", "@timestamp").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(2, list.size());
            assertEquals("test1.txt", list.get(0).toString());
            assertEquals("dir1/dir2/test3.txt", list.get(1).toString());
        }

        try (CurlResponse response = EcrCurl.delete(node, "/_configsync/file").header("Content-Type", "application/json")
                .body("{\"path\":\"test1.txt\"}").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
            assertEquals("deleted", contentMap.get("result").toString());
        }

        try (CurlResponse response = EcrCurl.get(node, "/_configsync/file").header("Content-Type", "application/json").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(1, list.size());
            assertEquals("dir1/dir2/test3.txt", list.get(0).toString());
        }

        try (CurlResponse response = EcrCurl.delete(node, "/_configsync/file").header("Content-Type", "application/json")
                .body("{\"path\":\"test3.txt\"}").execute()) {
            Map<String, Object> contentMap = response.getContent(EcrCurl.jsonParser);
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
