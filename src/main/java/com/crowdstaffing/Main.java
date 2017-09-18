package com.crowdstaffing;

import com.crowdstaffing.models.MongoOplogRecord;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.stream.Stream;

public class Main {

    private static final CodecRegistry POJO_CODEC_REGISTRY = CodecRegistries.fromRegistries(
            MongoClient.getDefaultCodecRegistry(),
            CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build()));
    private static final MongoClientOptions SETTINGS = MongoClientOptions.builder().codecRegistry(POJO_CODEC_REGISTRY).build();
    private static final MongoClient MONGO_CLIENT = new MongoClient(Arrays.asList(
            new ServerAddress("localhost", 27017),
            new ServerAddress("localhost", 27018),
            new ServerAddress("localhost", 27019)), SETTINGS);
    private static final MongoDatabase DB = MONGO_CLIENT.getDatabase("local").withCodecRegistry(POJO_CODEC_REGISTRY);
    private static final MongoCollection<MongoOplogRecord> OPLOG_COLLECTION = DB.getCollection("oplog.rs", MongoOplogRecord.class);

    private static final String POSTGRES_DB_URL = "jdbc:postgresql://localhost:5432/crowdstaffing";
    private static final String POSTGRES_USERNAME = "cs";
    private static final String POSTGRES_PASSWORD = "";


    public static void main(String[] args) throws Exception {
        MongoOplogTailer mongoOplogTailer = new MongoOplogTailer(OPLOG_COLLECTION);
        Stream<MongoOplogRecord> stream = mongoOplogTailer.tail();

        Class.forName("org.postgresql.Driver");
        Connection connection = DriverManager.getConnection(POSTGRES_DB_URL, POSTGRES_USERNAME, POSTGRES_PASSWORD);
        connection.setAutoCommit(true);

        /*stream.forEach(mongoOplog -> {
            System.out.println(mongoOplog.toString());
        });*/

        PostgresOplogWriter postgresOplogWriter = new PostgresOplogWriter(connection);
        postgresOplogWriter.write(stream);
    }
}
