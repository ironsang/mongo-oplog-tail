package com.crowdstaffing.models;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;

import java.util.HashMap;
import java.util.Map;

public class MongoOplogRecord {

    private BsonTimestamp ts;
    private Long t;
    private Long h;
    private Integer v;
    private String op;
    private String ns;
    private Map<String, BsonValue> o2 = new HashMap<>();
    private Map<String, BsonValue> o = new HashMap<>();

    public BsonTimestamp getTs() {
        return ts;
    }

    public void setTs(BsonTimestamp ts) {
        this.ts = ts;
    }

    public Long getH() {
        return h;
    }

    public void setH(Long h) {
        this.h = h;
    }

    public String getNs() {
        return ns;
    }

    public void setNs(String ns) {
        this.ns = ns;
    }

    public Map<String, BsonValue> getO() {
        return o;
    }

    public void setO(Map<String, BsonValue> o) {
        this.o = o;
    }

    public Map<String, BsonValue> getO2() {
        return o2;
    }

    public void setO2(Map<String, BsonValue> o2) {
        this.o2 = o2;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public Long getT() {
        return t;
    }

    public void setT(Long t) {
        this.t = t;
    }

    public Integer getV() {
        return v;
    }

    public void setV(Integer v) {
        this.v = v;
    }

    public String getDb() {
        return this.ns.split("\\.")[0];
    }

    public String getCollection() {
        return this.ns.split("\\.")[1];
    }

    public String getO2Object() {
        BsonDocument output = new BsonDocument();
        o2.forEach(output::append);
        return output.toJson();
    }

    public String getOObject() {
        BsonDocument output = new BsonDocument();
        o.forEach(output::append);
        return output.toJson();
    }

    public boolean isInsert() {
        return "i".equals(op);
    }

    public boolean isUpdate() {
        return "u".equals(op);
    }

    public boolean isDelete() {
        return "d".equals(op);
    }

    @Override
    public String toString() {
        return "com.crowdstaffing.models.MongoOplogRecord{db=" + getDb() +
                ", collection=" + getCollection() +
                ", ts=" + ts +
                ", h=" + h +
                ", ns='" + ns + '\'' +
                ", o=" + getOObject() +
                ", op='" + op + '\'' +
                ", t=" + t +
                ", v=" + v +
                '}';
    }

}