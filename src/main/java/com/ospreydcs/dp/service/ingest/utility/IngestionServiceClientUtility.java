package com.ospreydcs.dp.service.ingest.utility;

import com.ospreydcs.dp.grpc.v1.ingestion.DpIngestionServiceGrpc;
import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;

import java.util.concurrent.TimeUnit;

public class IngestionServiceClientUtility {
    
    public static class IngestionServiceGrpcClient {

        // instance variables
        private ManagedChannel channel;
        
        // configuration
        public static final String CFG_KEY_PORT = "IngestionServer.port";
        public static final int DEFAULT_PORT = 50051;
        public static final String CFG_KEY_INGESTION_CONNECT_STRING = "GrpcClient.ingestionConnectString";
        public static final String DEFAULT_INGESTION_CONNECT_STRING = "localhost:50051";
        private static final String CFG_KEY_CLIENT_KEEP_ALIVE_TIME_SECONDS = "GrpcClient.keepAliveTimeSeconds";
        private static final int DEFAULT_CLIENT_KEEP_ALIVE_TIME_SECONDS = 45;
        private static final String CFG_KEY_CLIENT_KEEP_ALIVE_TIMEOUT_SECONDS = "GrpcClient.keepAliveTimeoutSeconds";
        private static final int DEFAULT_CLIENT_KEEP_ALIVE_TIMEOUT_SECONDS = 20;
        private static final String CFG_KEY_CLIENT_KEEP_ALIVE_WITHOUT_CALLS = "GrpcClient.keepAliveWithoutCalls";
        private static final boolean DEFAULT_CLIENT_KEEP_ALIVE_WITHOUT_CALLS = true;


        public IngestionServiceGrpcClient() {
            
            final String connectString = getConnectString();

            int keepAliveTimeSeconds = configMgr().getConfigInteger(
                    CFG_KEY_CLIENT_KEEP_ALIVE_TIME_SECONDS,
                    DEFAULT_CLIENT_KEEP_ALIVE_TIME_SECONDS
            );
            int keepAliveTimeoutSeconds = configMgr().getConfigInteger(
                    CFG_KEY_CLIENT_KEEP_ALIVE_TIMEOUT_SECONDS,
                    DEFAULT_CLIENT_KEEP_ALIVE_TIMEOUT_SECONDS
            );
            boolean keepAliveWithoutCalls = configMgr().getConfigBoolean(
                    CFG_KEY_CLIENT_KEEP_ALIVE_WITHOUT_CALLS,
                    DEFAULT_CLIENT_KEEP_ALIVE_WITHOUT_CALLS
            );
            
            this.channel = Grpc.newChannelBuilder(connectString, InsecureChannelCredentials.create())
                    .keepAliveTime(keepAliveTimeSeconds, TimeUnit.SECONDS)
                    .keepAliveTimeout(keepAliveTimeoutSeconds, TimeUnit.SECONDS)
                    .keepAliveWithoutCalls(keepAliveWithoutCalls)
                    .build();
        }

        public IngestionServiceGrpcClient(ManagedChannel channel) {
            this.channel = channel;
        }

        protected static ConfigurationManager configMgr() {
            return ConfigurationManager.getInstance();
        }

        private String getConnectString() {
            return configMgr().getConfigString(CFG_KEY_INGESTION_CONNECT_STRING, DEFAULT_INGESTION_CONNECT_STRING);
        }

        public Channel getChannel() {
            return channel;
        }

        public DpIngestionServiceGrpc.DpIngestionServiceStub newStub() {
            return DpIngestionServiceGrpc.newStub(channel);
        }
    }

}
