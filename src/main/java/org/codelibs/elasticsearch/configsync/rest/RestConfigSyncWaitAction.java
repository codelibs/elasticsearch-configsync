package org.codelibs.elasticsearch.configsync.rest;

import static org.elasticsearch.action.ActionListener.wrap;

import java.io.IOException;

import org.codelibs.elasticsearch.configsync.service.ConfigSyncService;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;

public class RestConfigSyncWaitAction extends RestConfigSyncAction {

    private final ConfigSyncService configSyncService;

    @Inject
    public RestConfigSyncWaitAction(final Settings settings, final RestController controller,
            final ConfigSyncService configSyncService) {
        this.configSyncService = configSyncService;

        controller.registerHandler(RestRequest.Method.GET, "/_configsync/wait", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        try {
            switch (request.method()) {
            case GET:
                final String status = request.param("status", "yellow");
                final String timeout = request.param("timeout", "30s");
                return channel -> configSyncService.waitForStatus(status, timeout,
                        wrap(response -> sendResponse(channel, null), e -> sendErrorResponse(channel, e)));
            default:
                return channel -> sendErrorResponse(channel, new ElasticsearchException("Unknown request type."));
            }
        } catch (final Exception e) {
            return channel -> sendErrorResponse(channel, e);
        }
    }

    @Override
    public String getName() {
        return "configsync_wait_action";
    }
}
