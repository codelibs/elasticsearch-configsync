package org.codelibs.elasticsearch.configsync.rest;

import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.elasticsearch.configsync.exception.IORuntimeException;
import org.codelibs.elasticsearch.configsync.exception.InvalidRequestException;
import org.codelibs.elasticsearch.configsync.service.ConfigSyncService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.lookup.SourceLookup;

public class RestConfigSyncFileAction extends BaseRestHandler {

    private final ConfigSyncService configSyncService;

    @Inject
    public RestConfigSyncFileAction(final Settings settings, final Client client, final RestController controller,
            final ConfigSyncService configSyncService) {
        super(settings, controller, client);
        this.configSyncService = configSyncService;

        controller.registerHandler(RestRequest.Method.GET, "/_configsync/file", this);
        controller.registerHandler(RestRequest.Method.POST, "/_configsync/file", this);
        controller.registerHandler(RestRequest.Method.DELETE, "/_configsync/file", this);
    }

    @Override
    protected void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        try {
            final BytesReference content = request.content();
            switch (request.method()) {
            case GET: {
                String path = request.param(ConfigSyncService.PATH);
                if (path == null && content != null && content.length() > 0) {
                    final Map<String, Object> sourceAsMap = SourceLookup.sourceAsMap(content);
                    path = (String) sourceAsMap.get(ConfigSyncService.PATH);
                }
                if (path == null) {
                    configSyncService.getPaths(request.paramAsInt("from", 0), request.paramAsInt("size", 10),
                            new ActionListener<List<String>>() {

                                @Override
                                public void onResponse(final List<String> response) {
                                    final Map<String, Object> params = new HashMap<>();
                                    params.put("path", response);
                                    sendResponse(channel, params);
                                }

                                @Override
                                public void onFailure(final Throwable t) {
                                    sendErrorResponse(channel, t);
                                }
                            });
                } else {
                    configSyncService.getContent(path, new ActionListener<byte[]>() {

                        @Override
                        public void onResponse(final byte[] configContent) {
                            if (configContent != null) {
                                channel.sendResponse(new BytesRestResponse(OK, "application/octet-stream", configContent));
                            } else {
                                channel.sendResponse(new BytesRestResponse(NOT_FOUND));
                            }
                        }

                        @Override
                        public void onFailure(final Throwable t) {
                            sendErrorResponse(channel, t);
                        }
                    });
                }
            }
                break;
            case POST: {
                if (content == null) {
                    throw new InvalidRequestException("content is empty.");
                }
                String path = request.param(ConfigSyncService.PATH);
                byte[] contentArray;
                if (path != null) {
                    contentArray = content.array();
                } else {
                    final Map<String, Object> sourceAsMap = SourceLookup.sourceAsMap(content);
                    path = (String) sourceAsMap.get(ConfigSyncService.PATH);
                    final String fileContent = (String) sourceAsMap.get(ConfigSyncService.CONTENT);
                    contentArray = Base64.decode(fileContent);
                }
                configSyncService.store(path, contentArray, new ActionListener<IndexResponse>() {

                    @Override
                    public void onResponse(final IndexResponse arg0) {
                        sendResponse(channel, null);
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        sendErrorResponse(channel, t);
                    }
                });
            }
                break;
            case DELETE: {
                String path = request.param(ConfigSyncService.PATH);
                if (path == null && content != null && content.length() > 0) {
                    final Map<String, Object> sourceAsMap = SourceLookup.sourceAsMap(content);
                    path = (String) sourceAsMap.get(ConfigSyncService.PATH);
                }
                if (path == null) {
                    sendErrorResponse(channel, new InvalidRequestException(ConfigSyncService.PATH + " is empty."));
                    return;
                }
                configSyncService.delete(path, new ActionListener<DeleteResponse>() {

                    @Override
                    public void onResponse(final DeleteResponse response) {
                        final Map<String, Object> params = new HashMap<>();
                        params.put("found", response.isFound());
                        sendResponse(channel, params);
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        sendErrorResponse(channel, t);
                    }
                });
            }
                break;

            default:
                sendErrorResponse(channel, new InvalidRequestException("Unknown request type."));
                break;
            }
        } catch (final Exception e) {
            sendErrorResponse(channel, e);
        }
    }

    private void sendResponse(final RestChannel channel, final Map<String, Object> params) {
        try {
            final XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            builder.field("acknowledged", true);
            if (params != null) {
                for (final Map.Entry<String, Object> entry : params.entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(OK, builder));
        } catch (final IOException e) {
            throw new IORuntimeException("Failed to create a resposne.", e);
        }
    }

    private void sendErrorResponse(final RestChannel channel, final Throwable t) {
        try {
            channel.sendResponse(new BytesRestResponse(channel, t));
        } catch (final Exception e) {
            logger.error("Failed to send a failure response.", e);
        }
    }

}
