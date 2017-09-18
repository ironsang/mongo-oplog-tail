package com.crowdstaffing;

import com.crowdstaffing.models.MongoOplogRecord;
import org.postgresql.util.PGobject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.stream.Stream;

public class PostgresOplogWriter {

    private Connection connection;

    public PostgresOplogWriter(Connection connection) {
        this.connection = connection;
    }

    public void write(Stream<MongoOplogRecord> stream) throws Exception {
        stream.forEach(mongoOplog -> {
            System.out.println(mongoOplog.toString());
            writeToDb(mongoOplog);
        });
        connection.close();
    }

    private void writeToDb(MongoOplogRecord mongoOplog) {
        try {
            String sql = "INSERT INTO oplog.mongo_oplog (h, t, ts, v, op, db, collection, o) " +
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setLong(1, mongoOplog.getH());
            preparedStatement.setLong(2, mongoOplog.getT());
            preparedStatement.setLong(3, mongoOplog.getTs().getValue());
            preparedStatement.setInt(4, mongoOplog.getV());
            preparedStatement.setString(5, mongoOplog.getOp());
            preparedStatement.setString(6, mongoOplog.getDb());
            preparedStatement.setString(7, mongoOplog.getCollection());

            PGobject jsonObject = new PGobject();
            jsonObject.setType("jsonb");
            jsonObject.setValue(mongoOplog.getObject());
            preparedStatement.setObject(8, jsonObject);

            preparedStatement.execute();
            preparedStatement.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
