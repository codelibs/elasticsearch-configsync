package org.codelibs.elasticsearch.configsync;

import java.util.Collection;

import org.codelibs.elasticsearch.configsync.module.ConfigSyncModule;
import org.codelibs.elasticsearch.configsync.rest.RestConfigSyncFileAction;
import org.codelibs.elasticsearch.configsync.rest.RestConfigSyncRestartAction;
import org.codelibs.elasticsearch.configsync.service.ConfigSyncService;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

public class ConfigSyncPlugin extends AbstractPlugin {
    @Override
    public String name() {
        return "ConfigSyncPlugin";
    }

    @Override
    public String description() {
        return "This is a elasticsearch-configsync plugin.";
    }

    // for Rest API
    public void onModule(final RestModule module) {
        module.addRestAction(RestConfigSyncFileAction.class);
        module.addRestAction(RestConfigSyncRestartAction.class);
    }

    // for Service
    @Override
    public Collection<Class<? extends Module>> modules() {
        final Collection<Class<? extends Module>> modules = Lists.newArrayList();
        modules.add(ConfigSyncModule.class);
        return modules;
    }

    // for Service
    @SuppressWarnings("rawtypes")
    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        final Collection<Class<? extends LifecycleComponent>> services = Lists.newArrayList();
        services.add(ConfigSyncService.class);
        return services;
    }

}
