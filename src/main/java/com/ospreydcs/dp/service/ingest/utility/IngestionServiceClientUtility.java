package com.ospreydcs.dp.service.ingest.utility;

import com.ospreydcs.dp.grpc.v1.ingestion.DpIngestionServiceGrpc;
import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;

public class IngestionServiceClientUtility {
    
    public static class IngestionServiceClient {

        // instance variables
        private ManagedChannel channel;
        
        // configuration
        public static final String CFG_KEY_PORT = "IngestionServer.port";
        public static final int DEFAULT_PORT = 50051;
        public static final String CFG_KEY_HOSTNAME = "GrpcClient.hostname";
        public static final String DEFAULT_HOSTNAME = "localhost";

        public IngestionServiceClient() {
            final String connectString = getConnectString();
            this.channel = Grpc.newChannelBuilder(connectString, InsecureChannelCredentials.create()).build();
        }

        public IngestionServiceClient(ManagedChannel channel) {
            this.channel = channel;
        }

        protected static ConfigurationManager configMgr() {
            return ConfigurationManager.getInstance();
        }

        protected static int getPort() {
            return configMgr().getConfigInteger(CFG_KEY_PORT, DEFAULT_PORT);
        }

        protected static String getHostname() {
            return configMgr().getConfigString(CFG_KEY_HOSTNAME, DEFAULT_HOSTNAME);
        }
        
        private String getConnectString() {
            //     public static final String BENCHMARK_GRPC_CONNECT_STRING = "localhost:60051";
            return getHostname() + ":" + getPort();
        }

        public Channel getChannel() {
            return channel;
        }

        public DpIngestionServiceGrpc.DpIngestionServiceStub newStub() {
            return DpIngestionServiceGrpc.newStub(channel);
        }
    }

}
