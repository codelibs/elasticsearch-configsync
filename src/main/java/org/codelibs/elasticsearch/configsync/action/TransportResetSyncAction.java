/*
 * Copyright 2012-2022 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.elasticsearch.configsync.action;

import static org.elasticsearch.action.ActionListener.wrap;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Executor;

import org.codelibs.elasticsearch.configsync.service.ConfigSyncService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

public class TransportResetSyncAction extends HandledTransportAction<ResetSyncRequest, ResetSyncResponse> {

    private final TransportService transportService;

    private final ConfigSyncService configSyncService;

    @Inject
    public TransportResetSyncAction(final TransportService transportService, final ActionFilters actionFilters,
            final ConfigSyncService configSyncService) {
        super(ResetSyncAction.NAME, transportService, actionFilters, ResetSyncRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.transportService = transportService;
        this.configSyncService = configSyncService;
        configSyncService.setResetSyncAction(this);
    }

    @Override
    protected void doExecute(final Task task, final ResetSyncRequest request, final ActionListener<ResetSyncResponse> listener) {
        configSyncService.restartUpdater(wrap(response -> {
            listener.onResponse(new ResetSyncResponse(true));
        }, e -> {
            listener.onFailure(e);
        }));
    }

    public void sendRequest(final Iterator<DiscoveryNode> nodesIt, final ActionListener<ConfigResetSyncResponse> listener) {
        final DiscoveryNode node = nodesIt.next();
        transportService.sendRequest(node, ResetSyncAction.NAME, new ResetSyncRequest(), new TransportResponseHandler<ResetSyncResponse>() {

            @Override
            public ResetSyncResponse read(final StreamInput in) throws IOException {
                return new ResetSyncResponse(in);
            }

            @Override
            public void handleResponse(final ResetSyncResponse response) {
                configSyncService.resetSync(nodesIt, listener);
            }

            @Override
            public void handleException(final TransportException exp) {
                listener.onFailure(exp);
            }

            @Override
            public Executor executor() {
                return TRANSPORT_WORKER;
            }
        });
    }

}