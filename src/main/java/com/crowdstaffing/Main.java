package com.crowdstaffing;

import com.crowdstaffing.models.MongoOplogRecord;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.or;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class Main {
    public static void main(String[] args) throws Exception {
        // start mongo in replica mode with three replicas (on separate port)
        CodecRegistry pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        MongoClientOptions settings = MongoClientOptions.builder().codecRegistry(pojoCodecRegistry).build();

        MongoClient mongoClient = new MongoClient(Arrays.asList(
                new ServerAddress("localhost", 27017),
                new ServerAddress("localhost", 27018),
                new ServerAddress("localhost", 27019)), settings);

        MongoDatabase db = mongoClient.getDatabase("local").withCodecRegistry(pojoCodecRegistry);
        MongoCollection<MongoOplogRecord> oplogCollection = db.getCollection("oplog.rs", MongoOplogRecord.class);

        FindIterable<MongoOplogRecord> opCursor = oplogCollection
                .find(or(eq("op", "i"), eq("op", "u"), eq("op", "d")))
                .cursorType(CursorType.TailableAwait)
                .noCursorTimeout(true);

        MongoCursor<MongoOplogRecord> iterator = opCursor.iterator();
        Stream<MongoOplogRecord> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);

        stream.forEach(mongoOplog -> {
            System.out.println(mongoOplog.toString());
        });

        /*Class.forName("org.postgresql.Driver");
        Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/mongo_oplog", "", "");

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO tasks (name, question, type, accuracy) ");
        sql.append("VALUES(?, ?, ?, ?)");
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        preparedStatement.setString(1, name);
        preparedStatement.setString(2, question);
        preparedStatement.setInt(3, type);
        preparedStatement.setInt(4, accuracy);
        preparedStatement.execute();
        preparedStatement.close();

        connection.close();*/
    }
}
