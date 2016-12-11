package org.codelibs.elasticsearch.configsync;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.codelibs.elasticsearch.configsync.module.ConfigSyncModule;
import org.codelibs.elasticsearch.configsync.rest.RestConfigSyncFileAction;
import org.codelibs.elasticsearch.configsync.rest.RestConfigSyncFlushAction;
import org.codelibs.elasticsearch.configsync.rest.RestConfigSyncResetAction;
import org.codelibs.elasticsearch.configsync.rest.RestConfigSyncWaitAction;
import org.codelibs.elasticsearch.configsync.service.ConfigSyncService;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;

public class ConfigSyncPlugin extends Plugin implements ActionPlugin {

    @Override
    public List<Class<? extends RestHandler>> getRestHandlers() {
        return Arrays.asList(//
                RestConfigSyncFileAction.class, //
                RestConfigSyncResetAction.class, //
                RestConfigSyncFlushAction.class, //
                RestConfigSyncWaitAction.class);
    }

    @Override
    public Collection<Module> createGuiceModules() {
        final Collection<Module> modules = new ArrayList<Module>();
        modules.add(new ConfigSyncModule());
        return modules;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        final Collection<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        services.add(ConfigSyncService.class);
        return services;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(//
                ConfigSyncService.INDEX_SETTING, //
                ConfigSyncService.TYPE_SETTING, //
                ConfigSyncService.CONFIG_PATH_SETTING, //
                ConfigSyncService.SCROLL_TIME_SETTING, //
                ConfigSyncService.SCROLL_SIZE_SETTING, //
                ConfigSyncService.FLUSH_INTERVAL_SETTING, //
                ConfigSyncService.FILE_UPDATER_ENABLED_SETTING//
        );
    }

}
