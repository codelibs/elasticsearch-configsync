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

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.codelibs.core.lang.StringUtil;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "configsync")
public class ConfigSyncProperties {

    private final Elasticsearch elasticsearch = new Elasticsearch();

    public Elasticsearch getElasticsearch() {
        return elasticsearch;
    }

    public static class Elasticsearch {
        private String home;

        private List<String> modules = new ArrayList<>();

        public String getHome() {
            if (StringUtil.isBlank(home)) {
                return Paths.get("").toAbsolutePath().toString();
            }
            return home;
        }

        public void setHome(final String home) {
            this.home = home;
        }

        public List<String> getModules() {
            return modules;
        }

        public void setModules(final List<String> modules) {
            this.modules = modules;
        }

    }

}
