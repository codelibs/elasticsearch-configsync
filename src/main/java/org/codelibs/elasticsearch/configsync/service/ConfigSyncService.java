package org.codelibs.elasticsearch.configsync.service;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.codelibs.elasticsearch.configsync.action.ConfigFileFlushResponse;
import org.codelibs.elasticsearch.configsync.action.ConfigResetSyncResponse;
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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import com.google.common.base.Charsets;
import com.google.common.collect.UnmodifiableIterator;

public class ConfigSyncService extends AbstractLifecycleComponent<ConfigSyncService> {

    public static final String ACTION_CONFIG_FLUSH = "internal:indices/config/flush";

    public static final String ACTION_CONFIG_RESET = "internal:indices/config/reset_sync";

    private static final String FILE_MAPPING_JSON = "configsync/file_mapping.json";

    public static final String TIMESTAMP = "@timestamp";

    public static final String CONTENT = "content";

    public static final String PATH = "path";

    private final Client client;

    private final String index;

    private final String type;

    private final String configPath;

    private final ThreadPool threadPool;

    private final String scrollForUpdate;

    private final int sizeForUpdate;

    private Date lastChecked = new Date(0);

    private ConfigFileUpdater configFileUpdater;

    private final ClusterService clusterService;

    private final TransportService transportService;

    private volatile ScheduledFuture<?> scheduledFuture;

    @Inject
    public ConfigSyncService(final Settings settings, final Client client, final ClusterService clusterService,
            final TransportService transportService, final Environment env, final ThreadPool threadPool) {
        super(settings);
        this.client = client;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.threadPool = threadPool;

        logger.info("Creating ConfigSyncService");

        index = settings.get("configsync.index", ".configsync");
        type = settings.get("configsync.type", "file");
        configPath = settings.get("configsync.config_path", env.configFile().toFile().getAbsolutePath());
        scrollForUpdate = settings.get("configsync.scroll_time", "1m");
        sizeForUpdate = settings.getAsInt("configsync.scroll_size", 1);

        transportService.registerRequestHandler(ACTION_CONFIG_FLUSH,FileFlushRequest.class,ThreadPool.Names.GENERIC, new ConfigFileFlushRequestHandler());
        transportService.registerRequestHandler(ACTION_CONFIG_RESET,ResetSyncRequest.class,ThreadPool.Names.GENERIC, new ConfigSyncResetRequestHandler());
    }

    private void startUpdater() {
        configFileUpdater = new ConfigFileUpdater();

        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }

        final TimeValue flushInterval =
                clusterService.state().getMetaData().settings().getAsTime("configsync.flush_interval", TimeValue.timeValueMinutes(1));
        scheduledFuture = threadPool.schedule(flushInterval, Names.SAME, configFileUpdater);
        if (logger.isDebugEnabled()) {
            logger.debug("Scheduled ConfigFileUpdater with " + flushInterval);
        }
        logger.info("Scheduled ConfigFileUpdater with " + flushInterval);
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
                if (e instanceof IndexNotFoundException) {
                    createIndex();
                } else {
                    logger.error("Failed to start ConfigSyncService.", e);
                }
            }
        });
    }

    private void createIndex() {
        try {
            final String source = Streams.copyToString(new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(FILE_MAPPING_JSON), Charsets.UTF_8));
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
        configFileUpdater.terminate();
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

    public void getPaths(final int from, final int size, final String[] fields, final String sortField, final String sortOrder,
            final ActionListener<List<Object>> listener) {
        final boolean hasFields = !(fields == null || fields.length == 0);
        client.prepareSearch(index).setTypes(type).setSize(size).setFrom(from).addFields(hasFields ? fields : new String[] { PATH })
                .addSort(sortField, SortOrder.DESC.toString().equalsIgnoreCase(sortOrder) ? SortOrder.DESC : SortOrder.ASC)
                .execute(new ActionListener<SearchResponse>() {

                    @Override
                    public void onResponse(final SearchResponse response) {
                        final List<Object> objList = new ArrayList<>();
                        for (final SearchHit hit : response.getHits().getHits()) {
                            if (hasFields) {
                                Map<String, Object> objMap = new HashMap<>();
                                for (final String field : fields) {
                                    objMap.put(field, hit.getFields().get(field).getValue());
                                }
                                objList.add(objMap);
                            } else {
                                objList.add(hit.getFields().get(PATH).getValue());
                            }
                        }
                        listener.onResponse(objList);
                    }

                    @Override
                    public void onFailure(final Throwable e) {
                        listener.onFailure(e);
                    }
                });
    }

    private String getId(final String path) {
        return Base64.encodeBytes(path.getBytes(Charsets.UTF_8));
    }

    public void resetSync(final ActionListener<ConfigResetSyncResponse> listener) {
        final ClusterState state = clusterService.state();
        final DiscoveryNodes nodes = state.nodes();
        final UnmodifiableIterator<DiscoveryNode> nodesIt = nodes.dataNodes().valuesIt();
        resetSync(nodesIt, listener);
    }

    private void resetSync(final UnmodifiableIterator<DiscoveryNode> nodesIt, final ActionListener<ConfigResetSyncResponse> listener) {
        if (!nodesIt.hasNext()) {
            listener.onResponse(new ConfigResetSyncResponse(true));
        } else {
            final DiscoveryNode node = nodesIt.next();
            transportService.sendRequest(node, ACTION_CONFIG_RESET, new ResetSyncRequest(),
                    new TransportResponseHandler<ResetSyncResponse>() {

                        @Override
                        public ResetSyncResponse newInstance() {
                            return new ResetSyncResponse();
                        }

                        @Override
                        public void handleResponse(final ResetSyncResponse response) {
                            resetSync(nodesIt, listener);
                        }

                        @Override
                        public void handleException(final TransportException exp) {
                            listener.onFailure(exp);
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.GENERIC;
                        }
                    });
        }
    }

    private void restartUpdater() {
        if (logger.isInfoEnabled()) {
            logger.info("Restarting ConfigFileUpdater...");
        }
        configFileUpdater.terminate();
        startUpdater();
    }

    public void flush(final ActionListener<ConfigFileFlushResponse> listener) {
        final ClusterState state = clusterService.state();
        final DiscoveryNodes nodes = state.nodes();
        final UnmodifiableIterator<DiscoveryNode> nodesIt = nodes.dataNodes().valuesIt();
        flushOnNode(nodesIt, listener);
    }

    private void flushOnNode(final UnmodifiableIterator<DiscoveryNode> nodesIt, final ActionListener<ConfigFileFlushResponse> listener) {
        if (!nodesIt.hasNext()) {
            listener.onResponse(new ConfigFileFlushResponse(true));
        } else {
            final DiscoveryNode node = nodesIt.next();
            transportService.sendRequest(node, ACTION_CONFIG_FLUSH, new FileFlushRequest(),
                    new TransportResponseHandler<FileFlushResponse>() {

                        @Override
                        public FileFlushResponse newInstance() {
                            return new FileFlushResponse();
                        }

                        @Override
                        public void handleResponse(final FileFlushResponse response) {
                            flushOnNode(nodesIt, listener);
                        }

                        @Override
                        public void handleException(final TransportException exp) {
                            listener.onFailure(exp);
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.GENERIC;
                        }
                    });
        }
    }

    public void getContent(final String path, final ActionListener<byte[]> listener) {
        client.prepareGet(index, type, getId(path)).execute(new ActionListener<GetResponse>() {

            @Override
            public void onResponse(final GetResponse response) {
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
            public void onFailure(final Throwable e) {
                listener.onFailure(e);
            }
        });
    }

    public void delete(final String path, final ActionListener<DeleteResponse> listener) {
        client.prepareDelete(index, type, getId(path)).setRefresh(true).execute(listener);
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
                decodeToFile(content, absolutePath);
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

    class ConfigFileUpdater implements Runnable {

        ConfigFileWriter writer = new ConfigFileWriter();

        @Override
        public void run() {
            if (writer.terminated.get()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Terminated " + this);
                }
                return;
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Processing ConfigFileUpdater.");
            }

            writer.execute(new ActionListener<Void>() {

                @Override
                public void onResponse(final Void response) {
                    startUpdater();
                }

                @Override
                public void onFailure(final Throwable e) {
                    logger.error("Failed to process ConfigFileUpdater.", e);
                    startUpdater();
                }
            });
        }

        public void terminate() {
            writer.terminate();
        }
    }

    class ConfigFileWriter implements ActionListener<SearchResponse> {

        private final AtomicBoolean initialized = new AtomicBoolean(false);

        private final AtomicBoolean terminated = new AtomicBoolean(false);

        private ActionListener<Void> listener;

        public void execute(final ActionListener<Void> listener) {
            this.listener = listener;

            final Date now = new Date();
            final QueryBuilder queryBuilder =QueryBuilders.boolQuery().filter(QueryBuilders.rangeQuery(TIMESTAMP).from(lastChecked));
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
                listener.onResponse(null);
            } else {
                for (final SearchHit hit : hits) {
                    final Map<String, Object> source = hit.getSource();
                    updateConfigFile(source);
                }
                client.prepareSearchScroll(scrollId).setScroll(scrollForUpdate).execute(this);
            }
        }

        @Override
        public void onFailure(final Throwable t) {
            listener.onFailure(t);
        }
    }

    class ConfigFileFlushRequestHandler implements TransportRequestHandler<FileFlushRequest> {

        @Override
        public void messageReceived(final FileFlushRequest request, final TransportChannel channel) throws Exception {
            new ConfigFileWriter().execute(new ActionListener<Void>() {

                @Override
                public void onResponse(final Void response) {
                    try {
                        channel.sendResponse(new FileFlushResponse(true));
                    } catch (final IOException e) {
                        throw new IORuntimeException("Failed to write a response.", e);
                    }
                }

                @Override
                public void onFailure(final Throwable e) {
                    logger.error("Failed to flush config files.", e);
                    try {
                        channel.sendResponse(e);
                    } catch (final IOException e1) {
                        throw new IORuntimeException("Failed to write a response.", e1);
                    }
                }
            });
        }
    }

    private static class FileFlushRequest extends TransportRequest {

        FileFlushRequest() {
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    private static class FileFlushResponse extends AcknowledgedResponse {

        FileFlushResponse() {
        }

        FileFlushResponse(final boolean acknowledged) {
            super(acknowledged);
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
            readAcknowledged(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            writeAcknowledged(out);
        }
    }

    class ConfigSyncResetRequestHandler implements TransportRequestHandler<ResetSyncRequest> {

        @Override
        public void messageReceived(final ResetSyncRequest request, final TransportChannel channel) throws Exception {
            restartUpdater();
            channel.sendResponse(new ResetSyncResponse(true));
        }
    }

    private static class ResetSyncRequest extends TransportRequest {

        ResetSyncRequest() {
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    private static class ResetSyncResponse extends AcknowledgedResponse {

        ResetSyncResponse() {
        }

        ResetSyncResponse(final boolean acknowledged) {
            super(acknowledged);
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
            readAcknowledged(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            writeAcknowledged(out);
        }
    }

    private static void decodeToFile(String dataToDecode, String filename)
            throws java.io.IOException {

        Base64.OutputStream bos = null;
        try {
            bos = new Base64.OutputStream(
                    new java.io.FileOutputStream(filename), Base64.DECODE);
            bos.write(dataToDecode.getBytes(Base64.PREFERRED_ENCODING));
        }   // end try
        catch (java.io.IOException e) {
            throw e; // Catch and throw to execute finally{} block
        }   // end catch: java.io.IOException
        finally {
            try {
                bos.close();
            } catch (Exception e) {
            }
        }   // end finally

    }   // end decodeToFile
}
