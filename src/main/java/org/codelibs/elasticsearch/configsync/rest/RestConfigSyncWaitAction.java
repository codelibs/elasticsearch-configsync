package org.codelibs.elasticsearch.configsync.rest;

import static org.elasticsearch.rest.RestStatus.OK;

import java.io.IOException;
import java.util.Map;

import org.codelibs.elasticsearch.configsync.service.ConfigSyncService;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;

public class RestConfigSyncWaitAction extends BaseRestHandler {

    private final ConfigSyncService configSyncService;

    @Inject
    public RestConfigSyncWaitAction(final Settings settings, final Client client, final RestController controller,
            final ConfigSyncService configSyncService) {
        super(settings, controller, client);
        this.configSyncService = configSyncService;

        controller.registerHandler(RestRequest.Method.GET, "/_configsync/wait", this);
    }

    @Override
    protected void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        try {
            switch (request.method()) {
            case GET: {
                configSyncService.waitForStatus(request.param("status", "yellow"), request.param("timeout", "30s"),
                        new ActionListener<ClusterHealthResponse>() {

                            @Override
                            public void onResponse(final ClusterHealthResponse response) {
                                sendResponse(channel, null);
                            }

                            @Override
                            public void onFailure(final Throwable e) {
                                sendErrorResponse(channel, e);
                            }
                        });
            }
                break;
            default:
                sendErrorResponse(channel, new ElasticsearchException("Unknown request type."));
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
            throw new ElasticsearchException("Failed to create a resposne.", e);
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
