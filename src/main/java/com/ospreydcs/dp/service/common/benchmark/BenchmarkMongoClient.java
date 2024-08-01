package com.ospreydcs.dp.service.common.benchmark;

import com.mongodb.client.MongoDatabase;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.common.mongo.MongoSyncClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BenchmarkMongoClient extends MongoSyncClient {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // constants
    public static final String BENCHMARK_DATABASE_NAME = "dp-benchmark";

    @Override
    public boolean init() {

        // override the default database name globally
        logger.info("overriding db name globally to: {}", BENCHMARK_DATABASE_NAME);
        MongoClientBase.setMongoDatabaseName(BENCHMARK_DATABASE_NAME);

        // init so we have database client for dropping existing db
        super.init();
        dropBenchmarkDatabase();
        super.fini();

        // re-initialize to recreate db and collections as needed
        return super.init();
    }

    public void dropBenchmarkDatabase() {
        logger.info("dropping database: {}", BENCHMARK_DATABASE_NAME);
        MongoDatabase database = this.mongoClient.getDatabase(BENCHMARK_DATABASE_NAME);
        database.drop();
    }

    public static void prepareBenchmarkDatabase() {
        BenchmarkMongoClient benchmarkClient = new BenchmarkMongoClient();
        benchmarkClient.init();
    }

}
