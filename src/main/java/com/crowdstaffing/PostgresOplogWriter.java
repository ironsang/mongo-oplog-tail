package com.crowdstaffing;

import com.crowdstaffing.models.MongoOplogRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.postgresql.util.PGobject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

public class PostgresOplogWriter {

    public static final ObjectMapper Json = new ObjectMapper();
    private Connection connection;

    public PostgresOplogWriter(Connection connection) {
        this.connection = connection;
    }

    public void write(Stream<MongoOplogRecord> stream) throws Exception {
        stream.filter(mongoOplog -> "crowdstaffing".equals(mongoOplog.getDb()))
                .forEach(mongoOplog -> {
                    System.out.println(mongoOplog.toString());
                    writeToDb(mongoOplog);
                    /*if ("talents".equals(mongoOplog.getCollection())) {
                        writeToTalentsTable(mongoOplog);
                    }*/

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
            jsonObject.setValue(mongoOplog.getOObject());
            preparedStatement.setObject(8, jsonObject);

            preparedStatement.execute();
            preparedStatement.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writeToTalentsTable(MongoOplogRecord mongoOplog) {
        try {
            JsonNode talent = Json.readTree(mongoOplog.getOObject());
            JsonNode o2Object = Json.readTree(mongoOplog.getO2Object());
            if (mongoOplog.isInsert()) {
                handleInsert(talent);
            } else if (mongoOplog.isUpdate()) {
                String talent_id = o2Object.get("_id").get("$oid").asText();
                if (talent.has("$set")) {
                    Iterator<String> setFieldNames = talent.get("$set").fieldNames();
                    String next = setFieldNames.next();
                    String fieldName = next.split("\\.")[0];
                    int arrPostition = Integer.parseInt(next.split("\\.")[1]);
                    String sql = "SELECT * FROM data.talents t WHERE t._id = ?";
                    PreparedStatement s = connection.prepareStatement(sql);
                    s.setString(1, talent_id);
                    ResultSet resultSet = s.executeQuery();
                    s.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleInsert(JsonNode talent) {
        try {
            String talent_id = talent.get("_id").get("$oid").asText();
            String city = talent.has("city") ? talent.get("city").asText() : null;
            List<String> jobIds = talent.findValues("job_ids").stream().findFirst().map(jn -> {
                Iterator<JsonNode> iterator = jn.iterator();
                Stream<JsonNode> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
                return stream.map(j -> j.get("$oid").asText()).collect(toList());
            }).orElse(new ArrayList<>());

            jobIds.forEach(jobId -> {
                String sql = "INSERT INTO data.talent_jobs (talent_id, job_id) VALUES(?, ?)";
                try {
                    PreparedStatement s = connection.prepareStatement(sql);
                    s.setString(1, talent_id);
                    s.setString(2, jobId);
                    s.execute();
                    s.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });

            String sql = "INSERT INTO data.talents (_id, city) VALUES(?, ?)";
            PreparedStatement s = connection.prepareStatement(sql);

            s.setString(1, talent_id);

            s.setString(2, city);
            s.execute();
            s.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
