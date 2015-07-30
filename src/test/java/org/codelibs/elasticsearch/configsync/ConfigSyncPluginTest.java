package org.codelibs.elasticsearch.configsync;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.codelibs.elasticsearch.runner.net.Curl;
import org.codelibs.elasticsearch.runner.net.CurlResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import junit.framework.TestCase;

public class ConfigSyncPluginTest extends TestCase {

    private ElasticsearchClusterRunner runner;

    private int numOfNode = 3;

    private File[] configFiles;

    @Override
    protected void setUp() throws Exception {
        // create runner instance
        runner = new ElasticsearchClusterRunner();
        // create ES nodes
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
                settingsBuilder.put("http.cors.enabled", true);
                settingsBuilder.put("configsync.flush_interval", "1m");
            }
        }).build(newConfigs().clusterName("es-configsync-" + System.currentTimeMillis()).ramIndexStore().numOfNode(numOfNode));

        // wait for yellow status
        runner.ensureYellow();

        configFiles = null;

        for (int i = 0; i < 10; i++) {
            if (runner.indexExists(".configsync")) {
                break;
            }
            try {
                Thread.sleep(1000L);
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

    public void test_configFiles() throws Exception {
        Node node = runner.node();

        {
            Settings settings = ImmutableSettings.settingsBuilder().put("configsync.flush_interval", "1s").build();
            ClusterUpdateSettingsResponse response =
                    node.client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).execute().actionGet();
            assertTrue(response.isAcknowledged());
        }

        try (CurlResponse response = Curl.post(node, "/_configsync/reset").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        configFiles = new File[numOfNode * 3];
        for (int i = 0; i < numOfNode; i++) {
            String confPath = runner.getNode(i).settings().get("path.conf");

            int base = i * 3;
            configFiles[base] = new File(confPath, "test1.txt");
            configFiles[base + 1] = new File(confPath, "dir1/test2.txt");
            configFiles[base + 2] = new File(confPath, "dir1/dir2/test3.txt");
        }

        try (CurlResponse response = Curl.get(node, "/_configsync/file").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(0, list.size());
        }

        try (CurlResponse response = Curl.post(node, "/_configsync/file").param("path", "test1.txt").body("Test1").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        try (CurlResponse response = Curl.get(node, "/_configsync/file").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(1, list.size());
            assertEquals("test1.txt", list.get(0).toString());
        }

        Thread.sleep(1000L);

        for (int i = 0; i < numOfNode; i++) {
            int base = i * 3;
            assertTrue(configFiles[base].exists());
            assertFalse(configFiles[base + 1].exists());
            assertFalse(configFiles[base + 2].exists());

            assertEquals("Test1", new String(getText(configFiles[base])));
        }

        try (CurlResponse response = Curl.post(node, "/_configsync/file")
                .body("{\"path\":\"dir1/test2.txt\",\"content\":\"" + Base64.encodeBytes("Test2".getBytes(Charsets.UTF_8)) + "\"}")
                .execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        try (CurlResponse response = Curl.get(node, "/_configsync/file").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(2, list.size());
            assertEquals("test1.txt", list.get(0).toString());
            assertEquals("dir1/test2.txt", list.get(1).toString());
        }

        Thread.sleep(1000L);

        for (int i = 0; i < numOfNode; i++) {
            int base = i * 3;
            assertTrue(configFiles[base].exists());
            assertTrue(configFiles[base + 1].exists());
            assertFalse(configFiles[base + 2].exists());

            assertEquals("Test1", new String(getText(configFiles[base])));
            assertEquals("Test2", new String(getText(configFiles[base + 1])));
        }

        try (CurlResponse response = Curl.post(node, "/_configsync/file")
                .body("{\"path\":\"dir1/dir2/test3.txt\",\"content\":\"" + Base64.encodeBytes("Test3".getBytes(Charsets.UTF_8)) + "\"}")
                .execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        try (CurlResponse response = Curl.get(node, "/_configsync/file").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(3, list.size());
            assertEquals("test1.txt", list.get(0).toString());
            assertEquals("dir1/test2.txt", list.get(1).toString());
            assertEquals("dir1/dir2/test3.txt", list.get(2).toString());
        }

        Thread.sleep(1000L);

        for (int i = 0; i < numOfNode; i++) {
            int base = i * 3;
            assertTrue(configFiles[base].exists());
            assertTrue(configFiles[base + 1].exists());
            assertTrue(configFiles[base + 2].exists());

            assertEquals("Test1", new String(getText(configFiles[base])));
            assertEquals("Test2", new String(getText(configFiles[base + 1])));
            assertEquals("Test3", new String(getText(configFiles[base + 2])));
        }

        try (CurlResponse response = Curl.delete(node, "/_configsync/file").param("path", "dir1/test2.txt").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
            assertEquals("true", contentMap.get("found").toString());
        }

        try (CurlResponse response = Curl.get(node, "/_configsync/file").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(2, list.size());
            assertEquals("test1.txt", list.get(0).toString());
            assertEquals("dir1/dir2/test3.txt", list.get(1).toString());
        }

        try (CurlResponse response = Curl.delete(node, "/_configsync/file").body("{\"path\":\"test1.txt\"}").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
            assertEquals("true", contentMap.get("found").toString());
        }

        try (CurlResponse response = Curl.get(node, "/_configsync/file").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(1, list.size());
            assertEquals("dir1/dir2/test3.txt", list.get(0).toString());
        }

        try (CurlResponse response = Curl.delete(node, "/_configsync/file").body("{\"path\":\"test3.txt\"}").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
            assertEquals("false", contentMap.get("found").toString());
        }
    }

    public void test_configFiles_withFlush() throws Exception {
        Node node = runner.node();

        configFiles = new File[numOfNode * 3];
        for (int i = 0; i < numOfNode; i++) {
            String confPath = runner.getNode(i).settings().get("path.conf");

            int base = i * 3;
            configFiles[base] = new File(confPath, "test1.txt");
            configFiles[base + 1] = new File(confPath, "dir1/test2.txt");
            configFiles[base + 2] = new File(confPath, "dir1/dir2/test3.txt");
        }

        try (CurlResponse response = Curl.get(node, "/_configsync/file").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(0, list.size());
        }

        try (CurlResponse response = Curl.post(node, "/_configsync/file").param("path", "test1.txt").body("Test1").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        try (CurlResponse response = Curl.get(node, "/_configsync/file").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
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

        try (CurlResponse response = Curl.post(node, "/_configsync/flush").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        for (int i = 0; i < numOfNode; i++) {
            int base = i * 3;
            assertTrue(configFiles[base].exists());
            assertFalse(configFiles[base + 1].exists());
            assertFalse(configFiles[base + 2].exists());

            assertEquals("Test1", new String(getText(configFiles[base])));
        }

        try (CurlResponse response = Curl.post(node, "/_configsync/file")
                .body("{\"path\":\"dir1/test2.txt\",\"content\":\"" + Base64.encodeBytes("Test2".getBytes(Charsets.UTF_8)) + "\"}")
                .execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        try (CurlResponse response = Curl.get(node, "/_configsync/file").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(2, list.size());
            assertEquals("test1.txt", list.get(0).toString());
            assertEquals("dir1/test2.txt", list.get(1).toString());
        }

        try (CurlResponse response = Curl.post(node, "/_configsync/flush").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
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

        try (CurlResponse response = Curl.post(node, "/_configsync/file")
                .body("{\"path\":\"dir1/dir2/test3.txt\",\"content\":\"" + Base64.encodeBytes("Test3".getBytes(Charsets.UTF_8)) + "\"}")
                .execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
        }

        try (CurlResponse response = Curl.get(node, "/_configsync/file").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(3, list.size());
            assertEquals("test1.txt", list.get(0).toString());
            assertEquals("dir1/test2.txt", list.get(1).toString());
            assertEquals("dir1/dir2/test3.txt", list.get(2).toString());
        }

        try (CurlResponse response = Curl.post(node, "/_configsync/flush").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
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

        try (CurlResponse response = Curl.delete(node, "/_configsync/file").param("path", "dir1/test2.txt").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
            assertEquals("true", contentMap.get("found").toString());
        }

        try (CurlResponse response = Curl.get(node, "/_configsync/file").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(2, list.size());
            assertEquals("test1.txt", list.get(0).toString());
            assertEquals("dir1/dir2/test3.txt", list.get(1).toString());
        }

        try (CurlResponse response = Curl.delete(node, "/_configsync/file").body("{\"path\":\"test1.txt\"}").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
            assertEquals("true", contentMap.get("found").toString());
        }

        try (CurlResponse response = Curl.get(node, "/_configsync/file").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
            List<String> list = (List<String>) contentMap.get("path");
            assertEquals(1, list.size());
            assertEquals("dir1/dir2/test3.txt", list.get(0).toString());
        }

        try (CurlResponse response = Curl.delete(node, "/_configsync/file").body("{\"path\":\"test3.txt\"}").execute()) {
            Map<String, Object> contentMap = response.getContentAsMap();
            assertEquals("true", contentMap.get("acknowledged").toString());
            assertEquals("false", contentMap.get("found").toString());
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
