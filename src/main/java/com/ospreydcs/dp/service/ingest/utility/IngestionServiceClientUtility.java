package com.ospreydcs.dp.service.ingest.utility;

import com.ospreydcs.dp.grpc.v1.ingestion.DpIngestionServiceGrpc;
import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;

public class IngestionServiceClientUtility {
    
    public static class IngestionServiceClient {

        // static variables
        protected static volatile IngestionServiceClient instance; // singleton pattern
        private static Object mutex = new Object(); // singleton pattern
        private static ManagedChannel defaultChannel = null;
        
        // instance variables
        private ManagedChannel channel;
        
        // configuration
        public static final String CFG_KEY_PORT = "IngestionServer.port";
        public static final int DEFAULT_PORT = 50051;
        public static final String CFG_KEY_HOSTNAME = "GrpcClient.hostname";
        public static final String DEFAULT_HOSTNAME = "localhost";

        protected IngestionServiceClient() {
            // singleton pattern
        }

        public static IngestionServiceClient getInstance() {
            // singleton pattern
            IngestionServiceClient client = instance;
            if (client == null) {
                synchronized (mutex) {
                    client = instance;
                    if (client == null) {
                        instance = client = new IngestionServiceClient();
                        instance.initialize();
                    }
                }
            }
            return client;
        }

        private void initialize() {
            if (defaultChannel != null) {
                this.channel = defaultChannel;
            } else {
                final String connectString = getConnectString();
                this.channel = Grpc.newChannelBuilder(connectString, InsecureChannelCredentials.create()).build();
            }
        }

        protected static ConfigurationManager configMgr() {
            return ConfigurationManager.getInstance();
        }

        public static void setDefaultChannel(ManagedChannel channel) {
            defaultChannel = channel;
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
