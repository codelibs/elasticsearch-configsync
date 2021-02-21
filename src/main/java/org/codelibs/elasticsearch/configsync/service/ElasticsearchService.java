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
package org.codelibs.elasticsearch.configsync.service;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.CONFIG_DIR;
import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.ELASTICSEARCH_YAML;
import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.LOG4J2_PROPERTIES;
import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.elasticsearch.configsync.ConfigSyncConstants;
import org.codelibs.elasticsearch.configsync.ConfigSyncProperties;
import org.codelibs.elasticsearch.configsync.exception.ConfigSyncSystemException;
import org.codelibs.elasticsearch.runner.ClusterRunnerException;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.common.settings.Settings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ElasticsearchService {
    private static Logger logger = LogManager.getLogger(ElasticsearchService.class);

    @Autowired
    ConfigSyncProperties configSyncProperties;

    private ElasticsearchClusterRunner runner;

    private Consumer<Settings.Builder> overwriteSettingsBuilder;

    @PostConstruct
    public void initialize() {
        if (logger.isDebugEnabled()) {
            logger.debug("initializing ElasticsearchService");
        }

        final Path homePath = getHomePath();
        final Path confPath = homePath.resolve("node_1").resolve(CONFIG_DIR);
        createDir(confPath);

        final Path esConfPath = confPath.resolve(ELASTICSEARCH_YAML);
        if (!esConfPath.toFile().exists()) {
            try (InputStream is =
                    Thread.currentThread().getContextClassLoader().getResourceAsStream("elasticsearch/" + ELASTICSEARCH_YAML)) {
                Files.copy(is, esConfPath, StandardCopyOption.REPLACE_EXISTING);
            } catch (final IOException e) {
                throw new ClusterRunnerException("Could not create: " + esConfPath, e);
            }
        }

        final Path logConfPath = confPath.resolve(LOG4J2_PROPERTIES);
        if (!logConfPath.toFile().exists()) {
            try (InputStream is =
                    Thread.currentThread().getContextClassLoader().getResourceAsStream("elasticsearch/" + LOG4J2_PROPERTIES)) {
                Files.copy(is, logConfPath, StandardCopyOption.REPLACE_EXISTING);
            } catch (final IOException e) {
                throw new ClusterRunnerException("Could not create: " + logConfPath, e);
            }
        }

        final String clusterName = System.getProperty(ConfigSyncConstants.ELASTICSEARCH_CLUSTER_NAME, "elasticsearch");
        runner = new ElasticsearchClusterRunner();
        // create ES nodes
        getRunner().onBuild((id, builder) -> {
            try (InputStream is =
                    Thread.currentThread().getContextClassLoader().getResourceAsStream("elasticsearch/" + ELASTICSEARCH_YAML)) {
                builder.loadFromStream(ELASTICSEARCH_YAML, is, false);
            } catch (final IOException e) {
                throw new ClusterRunnerException("Could not load " + ELASTICSEARCH_YAML, e);
            }
            if (overwriteSettingsBuilder != null) {
                overwriteSettingsBuilder.accept(builder);
            }
        }).build(newConfigs()//
                .basePath(homePath.toAbsolutePath().toString())//
                .clusterName(clusterName)//
                .numOfNode(1)//
                .moduleTypes(configSyncProperties.getElasticsearch().getModules().stream().collect(Collectors.joining(",")))//
                .useLogger());

        // wait for yellow status
        getRunner().ensureYellow();
    }

    @PreDestroy
    public void destroy() {
        if (getRunner() != null && !getRunner().isClosed()) {
            try {
                getRunner().close();
            } catch (final IOException e) {
                logger.warn("Failed to close runner.", e);
            }
        }
    }

    private Path getHomePath() {
        return Paths.get(configSyncProperties.getElasticsearch().getHome());
    }

    protected void createDir(final Path path) {
        if (!path.toFile().exists()) {
            try {
                Files.createDirectories(path);
            } catch (final IOException e) {
                throw new ConfigSyncSystemException("Failed to create " + path, e);
            }
        }
    }

    public ElasticsearchClusterRunner getRunner() {
        return runner;
    }

    public void setOverwriteSettingsBuilder(final Consumer<Settings.Builder> overwriteSettingsBuilder) {
        this.overwriteSettingsBuilder = overwriteSettingsBuilder;
    }

}
