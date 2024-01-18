/*
 * Copyright 2012-2022 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.elasticsearch.configsync;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import org.codelibs.elasticsearch.configsync.action.FileFlushAction;
import org.codelibs.elasticsearch.configsync.action.ResetSyncAction;
import org.codelibs.elasticsearch.configsync.action.TransportFileFlushAction;
import org.codelibs.elasticsearch.configsync.action.TransportResetSyncAction;
import org.codelibs.elasticsearch.configsync.rest.RestConfigSyncFileAction;
import org.codelibs.elasticsearch.configsync.rest.RestConfigSyncFlushAction;
import org.codelibs.elasticsearch.configsync.rest.RestConfigSyncResetAction;
import org.codelibs.elasticsearch.configsync.rest.RestConfigSyncWaitAction;
import org.codelibs.elasticsearch.configsync.service.ConfigSyncService;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

public class ConfigSyncPlugin extends Plugin implements ActionPlugin {

    ConfigSyncService service;

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(//
                new ActionHandler<>(FileFlushAction.INSTANCE, TransportFileFlushAction.class), //
                new ActionHandler<>(ResetSyncAction.INSTANCE, TransportResetSyncAction.class));
    }

    @Override
    public List<RestHandler> getRestHandlers(final Settings settings, final RestController restController,
            final ClusterSettings clusterSettings, final IndexScopedSettings indexScopedSettings, final SettingsFilter settingsFilter,
            final IndexNameExpressionResolver indexNameExpressionResolver, final Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(//
                new RestConfigSyncFileAction(settings, restController, service), //
                new RestConfigSyncResetAction(settings, restController, service), //
                new RestConfigSyncFlushAction(settings, restController, service), //
                new RestConfigSyncWaitAction(settings, restController, service));
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        final Collection<Object> components = new ArrayList<>();
        service = new ConfigSyncService(services.client(), services.clusterService(), services.environment(), services.threadPool());
        components.add(service);
        return components;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(//
                ConfigSyncService.INDEX_SETTING, //
                ConfigSyncService.XPACK_SECURITY_USER_SETTING, //
                ConfigSyncService.XPACK_SECURITY_PASSWORD_SETTING, //
                ConfigSyncService.CONFIG_PATH_SETTING, //
                ConfigSyncService.SCROLL_TIME_SETTING, //
                ConfigSyncService.SCROLL_SIZE_SETTING, //
                ConfigSyncService.FLUSH_INTERVAL_SETTING, //
                ConfigSyncService.FILE_UPDATER_ENABLED_SETTING//
        );
    }

}
