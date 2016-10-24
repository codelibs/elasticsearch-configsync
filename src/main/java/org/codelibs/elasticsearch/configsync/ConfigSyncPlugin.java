package org.codelibs.elasticsearch.configsync;

import java.util.Collection;

import org.codelibs.elasticsearch.configsync.module.ConfigSyncModule;
import org.codelibs.elasticsearch.configsync.rest.RestConfigSyncFileAction;
import org.codelibs.elasticsearch.configsync.rest.RestConfigSyncFlushAction;
import org.codelibs.elasticsearch.configsync.rest.RestConfigSyncResetAction;
import org.codelibs.elasticsearch.configsync.rest.RestConfigSyncWaitAction;
import org.codelibs.elasticsearch.configsync.service.ConfigSyncService;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.settings.Validator;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestModule;

import com.google.common.collect.Lists;

public class ConfigSyncPlugin extends Plugin {
    @Override
    public String name() {
        return "configsync";
    }

    @Override
    public String description() {
        return "ConfigSync plugin syncs up with configuration files in .configsync index.";
    }

    // for Rest API
    public void onModule(final RestModule module) {
        module.addRestAction(RestConfigSyncFileAction.class);
        module.addRestAction(RestConfigSyncResetAction.class);
        module.addRestAction(RestConfigSyncFlushAction.class);
        module.addRestAction(RestConfigSyncWaitAction.class);
    }

    public void onModule(final ClusterModule module) {
        module.registerClusterDynamicSetting("configsync.flush_interval", Validator.TIME);
    }

    // for Service
    @Override
    public Collection<Module> nodeModules() {
        final Collection<Module> modules = Lists.newArrayList();
        modules.add(new ConfigSyncModule());
        return modules;
    }

    // for Service
    @SuppressWarnings("rawtypes")
    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        final Collection<Class<? extends LifecycleComponent>> services = Lists.newArrayList();
        services.add(ConfigSyncService.class);
        return services;
    }

}
