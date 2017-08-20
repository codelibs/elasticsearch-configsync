package org.codelibs.elasticsearch.configsync.rest;

import static org.elasticsearch.rest.RestStatus.OK;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;

public abstract class RestConfigSyncAction extends BaseRestHandler {

    protected RestConfigSyncAction(final Settings settings) {
        super(settings);
    }

    protected void sendResponse(final RestChannel channel, final Map<String, Object> params) {
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

    protected void sendErrorResponse(final RestChannel channel, final Exception e) {
        try {
            channel.sendResponse(new BytesRestResponse(channel, e));
        } catch (final Exception e1) {
            logger.error("Failed to send a failure response.", e1);
        }
    }
}
