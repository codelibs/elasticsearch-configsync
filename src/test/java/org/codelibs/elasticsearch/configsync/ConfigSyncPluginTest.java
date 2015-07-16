package org.codelibs.elasticsearch.configsync;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.node.Node;

import junit.framework.TestCase;

public class ConfigSyncPluginTest extends TestCase {

    private ElasticsearchClusterRunner runner;

    private int numOfNode = 2;

    @Override
    protected void setUp() throws Exception {
        // create runner instance
        runner = new ElasticsearchClusterRunner();
        // create ES nodes
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
                settingsBuilder.put("http.cors.enabled", true);
            }
        }).build(newConfigs().clusterName("es-configsync-" + System.currentTimeMillis()).ramIndexStore().numOfNode(numOfNode));

        // wait for yellow status
        runner.ensureYellow();
    }

    @Override
    protected void tearDown() throws Exception {
        // close runner
        runner.close();
        // delete all files
        runner.clean();
    }

    public void test_runCluster() throws Exception {
        Node node = runner.node();

        // TODO
    }

}
