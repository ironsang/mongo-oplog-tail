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
import org.junit.Test;

import java.util.Arrays;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.or;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class OplogParserTest {

    @Test
    public void testMongoOplogTailing() throws Exception {
        // start mongo in replica mode with three replicas (on separate port)
        CodecRegistry pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        MongoClientOptions settings = MongoClientOptions.builder().codecRegistry(pojoCodecRegistry).build();

        MongoClient mongoClient = new MongoClient(Arrays.asList(
                new ServerAddress("localhost", 27017),
                new ServerAddress("localhost", 27018),
                new ServerAddress("localhost", 27019)), settings);

        MongoDatabase db = mongoClient.getDatabase("local").withCodecRegistry(pojoCodecRegistry);
        MongoCollection<OplogRecord> oplogCollection = db.getCollection("oplog.rs", OplogRecord.class);

        FindIterable<OplogRecord> opCursor = oplogCollection
                .find(or(eq("op", "i"), eq("op", "u"), eq("op", "d")))
                .cursorType(CursorType.TailableAwait)
                .noCursorTimeout(true);

        MongoCursor<OplogRecord> iterator = opCursor.iterator();
        Stream<OplogRecord> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);

        stream.forEach(oplogRecord -> {
            System.out.println(oplogRecord.toString());
        });

    }
}
