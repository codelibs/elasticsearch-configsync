package org.codelibs.elasticsearch.configsync.module;

import org.codelibs.elasticsearch.configsync.service.ConfigSyncService;
import org.elasticsearch.common.inject.AbstractModule;

public class ConfigSyncModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(ConfigSyncService.class).asEagerSingleton();
    }
}