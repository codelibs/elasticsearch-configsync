package org.codelibs.elasticsearch.configsync.service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.codelibs.elasticsearch.configsync.exception.IORuntimeException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;

public class ConfigSyncService extends AbstractLifecycleComponent<ConfigSyncService> {

    public static final String TIMESTAMP = "@timestamp";

    public static final String CONTENT = "content";

    public static final String PATH = "path";

    private static final String FILE_MAPPING_JSON = "configsync/file_mapping.json";

    private final Client client;

    private final String index;

    private final String type;

    private final String configPath;

    private final ThreadPool threadPool;

    private final TimeValue flushInterval;

    private final String scrollForUpdate;

    private final int sizeForUpdate;

    private Date lastChecked = new Date(0);

    private ConfigFileUpdater configFileUpdater;

    private final ClusterService clusterService;

    @Inject
    public ConfigSyncService(final Settings settings, final Client client, final ClusterService clusterService, final Environment env,
            final ThreadPool threadPool) {
        super(settings);
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;

        logger.info("Creating ConfigSyncService");

        index = settings.get("configsync.index", ".configsync");
        type = settings.get("configsync.type", "file");
        configPath = settings.get("configsync.config_path", env.configFile().getAbsolutePath());
        flushInterval = settings.getAsTime("configsync.flush_interval", TimeValue.timeValueMinutes(1));
        scrollForUpdate = settings.get("configsync.scroll_time", "1m");
        sizeForUpdate = settings.getAsInt("configsync.scroll_size", 1);
    }

    private void startUpdater() {
        configFileUpdater = new ConfigFileUpdater();
        scheduleUpdater(configFileUpdater);
    }

    private void scheduleUpdater(final ConfigFileUpdater configFileUpdater) {
        threadPool.schedule(flushInterval, Names.SAME, configFileUpdater);
        if (logger.isDebugEnabled()) {
            logger.debug("Scheduled ConfigFileUpdater.");
        }
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        logger.info("Starting ConfigSyncService");

        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void afterStart() {
                client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute(new ActionListener<ClusterHealthResponse>() {

                    @Override
                    public void onResponse(final ClusterHealthResponse response) {
                        if (response.isTimedOut()) {
                            logger.error("Failed to start ConfigSyncService. Elasticsearch was timeouted.");
                        } else {
                            checkIfIndexExists();
                        }
                    }

                    @Override
                    public void onFailure(final Throwable e) {
                        logger.error("Failed to start ConfigSyncService.", e);
                    }
                });
            }
        });
    }

    private void checkIfIndexExists() {
        client.admin().indices().prepareExists(index).execute(new ActionListener<IndicesExistsResponse>() {

            @Override
            public void onResponse(final IndicesExistsResponse response) {
                if (response.isExists()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(index + " exists.");
                    }
                    startUpdater();
                } else {
                    createIndex();
                }
            }

            @Override
            public void onFailure(final Throwable e) {
                if (e instanceof IndexMissingException) {
                    createIndex();
                } else {
                    logger.error("Failed to start ConfigSyncService.", e);
                }
            }
        });
    }

    private void createIndex() {
        try {
            final String source = Streams.copyToStringFromClasspath(Thread.currentThread().getContextClassLoader(), FILE_MAPPING_JSON);
            client.admin().indices().prepareCreate(index).addMapping(type, source).execute(new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(final CreateIndexResponse response) {
                    startUpdater();
                }

                @Override
                public void onFailure(final Throwable e) {
                    logger.error("Failed to start ConfigSyncService.", e);
                }
            });
        } catch (final IOException e) {
            throw new IORuntimeException("Failed to access " + FILE_MAPPING_JSON, e);
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    public void store(final String path, final byte[] contentArray, final ActionListener<IndexResponse> listener) {
        try {
            final String id = getId(path);
            final XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            builder.field(PATH, path);
            builder.field(CONTENT, contentArray);
            builder.field(TIMESTAMP, new Date());
            client.prepareIndex(index, type, id).setSource(builder).setRefresh(true).execute(listener);
        } catch (final IOException e) {
            throw new IORuntimeException("Failed to register " + path, e);
        }
    }

    public void getPaths(int from, int size, final ActionListener<List<String>> listener) {
        client.prepareSearch(index).setTypes(type).setSize(size).setFrom(from).addField(PATH).addSort(TIMESTAMP, SortOrder.ASC)
                .execute(new ActionListener<SearchResponse>() {

                    @Override
                    public void onResponse(SearchResponse response) {
                        final List<String> pathList = new ArrayList<>();
                        for (final SearchHit hit : response.getHits().getHits()) {
                            pathList.add((String) hit.getFields().get(PATH).getValue());
                        }
                        listener.onResponse(pathList);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        listener.onFailure(e);
                    }
                });
    }

    private String getId(final String path) {
        return Base64.encodeBytes(path.getBytes(Charsets.UTF_8));
    }

    public void restartUpdater() {
        configFileUpdater.terminate();
        startUpdater();
    }

    private class ConfigFileUpdater implements ActionListener<SearchResponse>, Runnable {
        private final AtomicBoolean initialized = new AtomicBoolean(false);
        private final AtomicBoolean terminated = new AtomicBoolean(false);

        @Override
        public void run() {
            if (terminated.get()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Terminated " + this);
                }
                return;
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Processing ConfigFileUpdater.");
            }

            final Date now = new Date();
            final QueryBuilder queryBuilder =
                    QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), FilterBuilders.rangeFilter(TIMESTAMP).from(lastChecked));
            lastChecked = now;
            initialized.set(false);
            client.prepareSearch(index).setTypes(type).setSearchType(SearchType.SCAN).setQuery(queryBuilder).setScroll(scrollForUpdate)
                    .setSize(sizeForUpdate).execute(this);
        }

        public void terminate() {
            terminated.set(true);
        }

        @Override
        public void onResponse(final SearchResponse response) {
            if (terminated.get()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Terminated " + this);
                }
                return;
            }

            final String scrollId = response.getScrollId();
            if (!initialized.getAndSet(true)) {
                client.prepareSearchScroll(scrollId).setScroll(scrollForUpdate).execute(this);
                return;
            }

            final SearchHits searchHits = response.getHits();
            final SearchHit[] hits = searchHits.getHits();
            if (hits.length == 0) {
                scheduleUpdater(this);
                return;
            }
            for (final SearchHit hit : hits) {
                final Map<String, Object> source = hit.getSource();
                updateConfigFile(source);
            }
            client.prepareSearchScroll(scrollId).setScroll(scrollForUpdate).execute(this);
        }

        private void updateConfigFile(final Map<String, Object> source) {
            try {
                final Date timestamp = getTimestamp(source.get(TIMESTAMP));
                final String path = (String) source.get(PATH);
                final Path filePath = Paths.get(configPath, path.replace("..", ""));
                if (logger.isDebugEnabled()) {
                    logger.debug("Checking " + filePath);
                }
                if (!Files.exists(filePath) || Files.getLastModifiedTime(filePath).toMillis() < timestamp.getTime()) {
                    final String content = (String) source.get(CONTENT);
                    final File parentFile = filePath.toFile().getParentFile();
                    if (!parentFile.exists() && !parentFile.mkdirs()) {
                        logger.warn("Failed to create " + parentFile.getAbsolutePath());
                    }
                    final String absolutePath = filePath.toFile().getAbsolutePath();
                    Base64.decodeToFile(content, absolutePath);
                    logger.info("Updated " + absolutePath);
                }
            } catch (final Exception e) {
                logger.warn("Failed to update " + source.get(PATH), e);
            }
        }

        private Date getTimestamp(final Object value) throws ParseException {
            if (value instanceof Date) {
                return (Date) value;
            } else if (value instanceof Number) {
                return new Date(((Number) value).longValue());
            } else {
                final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                return sdf.parse(value.toString());
            }
        }

        @Override
        public void onFailure(final Throwable t) {
            logger.error("Failed to process ConfigFileUpdater.", t);
            scheduleUpdater(this);
        }
    }

    public void getContent(final String path, final ActionListener<byte[]> listener) {
        client.prepareGet(index, type, getId(path)).execute(new ActionListener<GetResponse>() {

            @Override
            public void onResponse(GetResponse response) {
                if (response.isExists()) {
                    try {
                        final byte[] configContent = Base64.decode((String) response.getSource().get(ConfigSyncService.CONTENT));
                        listener.onResponse(configContent);
                    } catch (final IOException e) {
                        throw new IORuntimeException("Failed to access the content.", e);
                    }
                } else {
                    listener.onResponse(null);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }

    public void delete(final String path, final ActionListener<DeleteResponse> listener) {
        client.prepareDelete(index, type, getId(path)).setRefresh(true).execute(listener);
    }
}
