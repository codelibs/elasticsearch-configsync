package org.codelibs.elasticsearch.configsync.rest;

import java.io.IOException;

import org.codelibs.elasticsearch.configsync.action.ConfigResetSyncResponse;
import org.codelibs.elasticsearch.configsync.service.ConfigSyncService;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;

public class RestConfigSyncResetAction extends RestConfigSyncAction {

    private final ConfigSyncService configSyncService;

    @Inject
    public RestConfigSyncResetAction(final Settings settings, final Client client, final RestController controller,
            final ConfigSyncService configSyncService) {
        super(settings);
        this.configSyncService = configSyncService;

        controller.registerHandler(RestRequest.Method.POST, "/_configsync/reset", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        try {
            switch (request.method()) {
            case POST:
                return channel -> configSyncService.resetSync(new ActionListener<ConfigResetSyncResponse>() {

                    @Override
                    public void onResponse(final ConfigResetSyncResponse response) {
                        sendResponse(channel, null);
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        sendErrorResponse(channel, e);
                    }
                });
            default:
                return channel -> sendErrorResponse(channel, new ElasticsearchException("Unknown request type."));
            }
        } catch (final Exception e) {
            return channel -> sendErrorResponse(channel, e);
        }
    }

}
