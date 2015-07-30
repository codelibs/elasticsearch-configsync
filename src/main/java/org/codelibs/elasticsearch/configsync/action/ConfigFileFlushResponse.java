package org.codelibs.elasticsearch.configsync.action;

import org.elasticsearch.action.support.master.AcknowledgedResponse;

public class ConfigFileFlushResponse extends AcknowledgedResponse {
    public ConfigFileFlushResponse(final boolean acknowledged) {
        super(acknowledged);
    }
}
