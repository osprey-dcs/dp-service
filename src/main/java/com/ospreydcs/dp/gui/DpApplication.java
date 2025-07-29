package com.ospreydcs.dp.gui;

import com.ospreydcs.dp.client.ApiClient;
import com.ospreydcs.dp.client.IngestionClient;
import com.ospreydcs.dp.grpc.v1.ingestion.RegisterProviderResponse;
import com.ospreydcs.dp.service.inprocess.InprocessServiceEcosystem;

public class DpApplication {

    // instance variables
    private InprocessServiceEcosystem inprocessServiceEcosystem = null;
    private ApiClient api = null;

    public boolean init() {

        // create InprocessServiceEcosystem with default local grpc targets
        inprocessServiceEcosystem = new InprocessServiceEcosystem();
        if (!inprocessServiceEcosystem.init()) {
            return false;
        }

        // initialize ApiClient with grpc targets from default inprocess service ecosystem
        api = new ApiClient(inprocessServiceEcosystem.ingestionService.getIngestionChannel());
        if (!api.init()) {
            return false;
        }

        return true;
    }

    public boolean fini() {
        api.fini();
        inprocessServiceEcosystem.fini();
        return true;
    }

    public void registerProvider() {

        // TODO: add stuff to params for provider

        final IngestionClient.RegisterProviderRequestParams params = null;

        // call registerProvider() API method
        final RegisterProviderResponse response = api.ingestionClient.sendRegsiterProvider(params);

        // TODO: handle return from sendRegisterProvider()

    }
}
