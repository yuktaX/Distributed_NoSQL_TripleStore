import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import java.io.*;
import java.text.ParseException;
import java.util.Map;
import java.util.Scanner;

public class MongoDB extends Server_sharded {

    private static final String CONNECTION_STRING = "mongodb://localhost:27017"; // Adjust if MongoDB runs on a different port
    private static final String DATABASE_NAME = "sample_yago";
    private static final String COLLECTION_NAME = "sample_yago";
    private static final int ID = 2;
    static long sequence_no;

    //data members
    private MongoClient mongoClient;
    private static MongoDatabase database;
    private static MongoCollection<Document> collection;

    static Map<Integer, Long[]> lastSyncedLines = Main.lastSyncedGlobal.get(2);

    public MongoDB() {
        this.mongoClient = MongoClients.create(CONNECTION_STRING);
        this.database = mongoClient.getDatabase(DATABASE_NAME);
        this.collection = database.getCollection(COLLECTION_NAME);
    }

    public static void updateTriple(MongoCollection<Document> collection, String subject, String predicate, String object, String time) throws IOException {

        Bson filter = Filters.and(Filters.eq("subject", subject), Filters.eq("predicate", predicate));
        Bson update = Updates.set("object", object);

        Document updatedDocument = collection.findOneAndUpdate(filter, update);
        if (updatedDocument == null) {
            System.out.println("No triple found for update. Creating a new one.");
            insertTriple(collection, subject, predicate, object);
        } else {
            System.out.println("Triple updated successfully!");
        }

        writeLog(ID, sequence_no, subject, predicate, object, time);
        sequence_no++;

//        //write to log file after update, get current timestamp if not a merge write
//        long timestamp;
//        if (time.length() == 0)
//            timestamp = System.currentTimeMillis();
//        else
//            timestamp = Long.parseLong(time);
//
//        // Format the update entry
//        String updateEntry = String.format("%d,%s,%s,%s,%d\n", sequence_no, subject, predicate, object, timestamp);
//        System.out.println(updateEntry);
//        sequence_no++;
//
//        // Write the update entry to a file (replace with your actual file path)
//        try (FileWriter writer = new FileWriter(Main.path + "server_2.txt", true)) {
//            writer.append(updateEntry);
//        }

    }

    public static void queryTriple(MongoCollection<Document> collection, Scanner scanner) {
        System.out.print("Enter subject to query: ");
        scanner.nextLine();
        String subject = scanner.nextLine(); // Consume extra newline

        Bson filter = subject.isEmpty() ? null : Filters.eq("subject", subject);
        for (Document document : collection.find(filter)) {
            String retrievedSubject = document.getString("subject");
            String retrievedPredicate = document.getString("predicate");
            String retrievedObject = document.getString("object");
            System.out.println("Subject: " + retrievedSubject + ", Predicate: " + retrievedPredicate + ", Object: " + retrievedObject);
        }
    }

    private static void insertTriple(MongoCollection<Document> collection, String subject, String predicate, String object) {
        Document document = new Document();
        document.put("subject", subject);
        document.put("predicate", predicate);
        document.put("object", object);
        collection.insertOne(document);
        System.out.println("New triple inserted!");
    }

    public static void merge(int serverId, MongoCollection<Document> collection) throws IOException, ParseException {

        // Merge logs
        Map<String, String[]> latestUpdates = mergeLogs(ID, serverId);

        Testing.printMergeMap(latestUpdates);

        for (Map.Entry<String, String[]> entry : latestUpdates.entrySet()) {
            String key = entry.getKey();
            String[] value = entry.getValue();

            // Split the key (subject,predicate) using comma separator
            String[] keyParts = key.split(",");
            if (keyParts.length != 2) {
                // Handle invalid key format (log a warning or ignore)
                System.err.println("Invalid key format in map: " + key);
                continue;
            }
            String subject = keyParts[0].trim();
            String predicate = keyParts[1].trim();

            // Extract object (first element of the value)
            String object = value[0];
            String time = value[1];
            String serverSource = value[2];

            // Call updateTriple with extracted subject, predicate, and object
            // Only call update if local server doesn't have most recent writes i.e serverSource = 1 indicating
            // remote server has most recent data
            if (serverSource.equals("1"))
                updateTriple(collection, subject, predicate, object, time);
        }
    }

    public MongoCollection<Document> getCollection() {
        return this.collection;
    }

    public static void start() {

        MongoCollection<Document> collection = database.getCollection("current_seq");
        Document document = collection.find().first();
        if (document != null) {
            Number seqNoNumber = (Number) document.get("seq_no");
            sequence_no = seqNoNumber.longValue();
        }
    }

    // Close method equivalent
    public void close() {
        MongoCollection<Document> collection = database.getCollection("current_seq");

        // Clear the collection
        collection.deleteMany(new Document());

        //temporary reset
        sequence_no = 1;

        // Insert new value
        Document newDocument = new Document("seq_no", sequence_no);
        collection.insertOne(newDocument);

        this.mongoClient.close();
    }

}

