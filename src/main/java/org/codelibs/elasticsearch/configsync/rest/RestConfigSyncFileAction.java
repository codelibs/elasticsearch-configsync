package org.codelibs.elasticsearch.configsync.rest;

import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.elasticsearch.configsync.service.ConfigSyncService;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.search.sort.SortOrder;

public class RestConfigSyncFileAction extends RestConfigSyncAction {

    private final ConfigSyncService configSyncService;

    @Inject
    public RestConfigSyncFileAction(final Settings settings, final RestController controller, final ConfigSyncService configSyncService) {
        super(settings);
        this.configSyncService = configSyncService;

        controller.registerHandler(RestRequest.Method.GET, "/_configsync/file", this);
        controller.registerHandler(RestRequest.Method.POST, "/_configsync/file", this);
        controller.registerHandler(RestRequest.Method.DELETE, "/_configsync/file", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        try {
            final BytesReference content = request.content();
            switch (request.method()) {
            case GET: {
                final String path;
                if (request.param(ConfigSyncService.PATH) != null) {
                    path = request.param(ConfigSyncService.PATH);
                } else if (content != null && content.length() > 0) {
                    final Map<String, Object> sourceAsMap = SourceLookup.sourceAsMap(content);
                    path = (String) sourceAsMap.get(ConfigSyncService.PATH);
                } else {
                    path = null;
                }
                if (path == null) {
                    final String[] sortValues = request.param("sort", ConfigSyncService.PATH).split(":");
                    final String sortField;
                    final String sortOrder;
                    if (sortValues.length > 1) {
                        sortField = sortValues[0];
                        sortOrder = sortValues[1];
                    } else {
                        sortField = sortValues[0];
                        sortOrder = SortOrder.ASC.toString();
                    }

                    final String[] fields = request.paramAsStringArrayOrEmptyIfAll("fields");
                    final int from = request.paramAsInt("from", 0);
                    final int size = request.paramAsInt("size", 10);
                    return channel -> configSyncService.getPaths(from, size, fields, sortField, sortOrder,
                            new ActionListener<List<Object>>() {

                                @Override
                                public void onResponse(final List<Object> response) {
                                    final Map<String, Object> params = new HashMap<>();
                                    params.put(fields.length == 0 ? "path" : "file", response);
                                    sendResponse(channel, params);
                                }

                                @Override
                                public void onFailure(final Exception e) {
                                    sendErrorResponse(channel, e);
                                }
                            });
                } else {
                    return channel -> configSyncService.getContent(path, new ActionListener<byte[]>() {

                        @Override
                        public void onResponse(final byte[] configContent) {
                            if (configContent != null) {
                                channel.sendResponse(new BytesRestResponse(OK, "application/octet-stream", configContent));
                            } else {
                                channel.sendResponse(new BytesRestResponse(NOT_FOUND, path + " is not found."));
                            }
                        }

                        @Override
                        public void onFailure(final Exception e) {
                            sendErrorResponse(channel, e);
                        }
                    });
                }
            }
            case POST: {
                if (content == null) {
                    throw new ElasticsearchException("content is empty.");
                }
                final String path;
                byte[] contentArray;
                if (request.param(ConfigSyncService.PATH) != null) {
                    path = request.param(ConfigSyncService.PATH);
                    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                        content.writeTo(out);
                        contentArray = out.toByteArray();
                    }
                } else {
                    final Map<String, Object> sourceAsMap = SourceLookup.sourceAsMap(content);
                    path = (String) sourceAsMap.get(ConfigSyncService.PATH);
                    final String fileContent = (String) sourceAsMap.get(ConfigSyncService.CONTENT);
                    contentArray = Base64.getDecoder().decode(fileContent);
                }
                return channel -> configSyncService.store(path, contentArray, new ActionListener<IndexResponse>() {

                    @Override
                    public void onResponse(final IndexResponse arg0) {
                        sendResponse(channel, null);
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        sendErrorResponse(channel, e);
                    }
                });
            }
            case DELETE: {
                final String path;
                if (request.param(ConfigSyncService.PATH) != null) {
                    path = request.param(ConfigSyncService.PATH);
                } else if (content != null && content.length() > 0) {
                    final Map<String, Object> sourceAsMap = SourceLookup.sourceAsMap(content);
                    path = (String) sourceAsMap.get(ConfigSyncService.PATH);
                } else {
                    path = null;
                }
                if (path == null) {
                    return channel -> sendErrorResponse(channel, new ElasticsearchException(ConfigSyncService.PATH + " is empty."));
                }
                return channel -> configSyncService.delete(path, new ActionListener<DeleteResponse>() {

                    @Override
                    public void onResponse(final DeleteResponse response) {
                        final Map<String, Object> params = new HashMap<>();
                        params.put("result", response.getResult().toString().toLowerCase());
                        sendResponse(channel, params);
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        sendErrorResponse(channel, e);
                    }
                });
            }
            default:
                return channel -> sendErrorResponse(channel, new ElasticsearchException("Unknown request type."));
            }
        } catch (final Exception e) {
            return channel -> sendErrorResponse(channel, e);
        }
    }

}
