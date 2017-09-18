package com.crowdstaffing;

import com.crowdstaffing.models.MongoOplogRecord;
import com.mongodb.CursorType;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.or;

public class MongoOplogTailer {
    private MongoCollection<MongoOplogRecord> oplogCollection;

    public MongoOplogTailer(MongoCollection<MongoOplogRecord> oplogCollection) {
        this.oplogCollection = oplogCollection;
    }

    public Stream<MongoOplogRecord> tail() {
        FindIterable<MongoOplogRecord> opCursor = oplogCollection
                .find(or(eq("op", "i"), eq("op", "u"), eq("op", "d")))
                .cursorType(CursorType.TailableAwait)
                .noCursorTimeout(true);

        MongoCursor<MongoOplogRecord> iterator = opCursor.iterator();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
    }
}
