package org.codelibs.elasticsearch.configsync.action;

import org.elasticsearch.action.support.master.AcknowledgedResponse;

public class ConfigResetSyncResponse extends AcknowledgedResponse {
    public ConfigResetSyncResponse(final boolean acknowledged) {
        super(acknowledged);
    }
}
