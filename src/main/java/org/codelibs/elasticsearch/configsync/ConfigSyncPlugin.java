package org.codelibs.elasticsearch.configsync;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import org.codelibs.elasticsearch.configsync.rest.RestConfigSyncFileAction;
import org.codelibs.elasticsearch.configsync.rest.RestConfigSyncFlushAction;
import org.codelibs.elasticsearch.configsync.rest.RestConfigSyncResetAction;
import org.codelibs.elasticsearch.configsync.rest.RestConfigSyncWaitAction;
import org.codelibs.elasticsearch.configsync.service.ConfigSyncService;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

public class ConfigSyncPlugin extends Plugin implements ActionPlugin {

    private final PluginComponent pluginComponent = new PluginComponent();

    @Override
    public List<RestHandler> getRestHandlers(final Settings settings, final RestController restController, final ClusterSettings clusterSettings,
            final IndexScopedSettings indexScopedSettings, final SettingsFilter settingsFilter, final IndexNameExpressionResolver indexNameExpressionResolver,
            final Supplier<DiscoveryNodes> nodesInCluster) {
        final ConfigSyncService service = pluginComponent.getConfigSyncService();
        return Arrays.asList(//
                new RestConfigSyncFileAction(settings, restController, service), //
                new RestConfigSyncResetAction(settings, restController, service), //
                new RestConfigSyncFlushAction(settings, restController, service), //
                new RestConfigSyncWaitAction(settings, restController, service));
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        final Collection<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        services.add(ConfigSyncService.class);
        return services;
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService, ScriptService scriptService, NamedXContentRegistry xContentRegistry,
            Environment environment, NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        final Collection<Object> components = new ArrayList<>();
        components.add(pluginComponent);
        return components;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(//
                ConfigSyncService.INDEX_SETTING, //
                ConfigSyncService.TYPE_SETTING, //
                ConfigSyncService.XPACK_SECURITY_SETTING, //
                ConfigSyncService.CONFIG_PATH_SETTING, //
                ConfigSyncService.SCROLL_TIME_SETTING, //
                ConfigSyncService.SCROLL_SIZE_SETTING, //
                ConfigSyncService.FLUSH_INTERVAL_SETTING, //
                ConfigSyncService.FILE_UPDATER_ENABLED_SETTING//
        );
    }

    public static class PluginComponent {
        private ConfigSyncService configSyncService;

        public ConfigSyncService getConfigSyncService() {
            return configSyncService;
        }

        public void setConfigSyncService(final ConfigSyncService configSyncService) {
            this.configSyncService = configSyncService;
        }
    }
}
