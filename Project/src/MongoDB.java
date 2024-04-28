import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class MongoDB {

    private static final String CONNECTION_STRING = "mongodb://localhost:27017"; // Adjust if MongoDB runs on a different port
    private static final String DATABASE_NAME = "sample_yago";
    private static final String COLLECTION_NAME = "sample_yago";
    private static final int ID = 2;
    static long sequence_no = 1;

    //data members
    private MongoClient mongoClient;
    private MongoDatabase database;
    private static MongoCollection<Document> collection;

    static Map<Integer, Long[]> lastSyncedLines = Main.lastSyncedGlobal.get(2);

    public MongoDB() {
        this.mongoClient = MongoClients.create(CONNECTION_STRING);
        this.database = mongoClient.getDatabase(DATABASE_NAME);
        this.collection = database.getCollection(COLLECTION_NAME);
    }

    public static void updateTriple(MongoCollection<Document> collection, String subject, String predicate, String object) throws IOException {

        //check if triplet already exists to avoid writing duplicates

        // Create a filter document to match the triple
        Document filter_check = new Document();
        filter_check.append("subject", subject);
        filter_check.append("predicate", predicate);
        filter_check.append("object", object);

        // Find one document matching the filter (returns null if not found)
        FindIterable<Document> results = collection.find(filter_check).limit(1);

        if(results.iterator().hasNext() == true){
            System.out.println("Object already present");
            return;
        }

        Bson filter = Filters.and(Filters.eq("subject", subject), Filters.eq("predicate", predicate));
        Bson update = Updates.set("object", object);

        Document updatedDocument = collection.findOneAndUpdate(filter, update);
        if (updatedDocument == null) {
            System.out.println("No triple found for update. Creating a new one.");
            insertTriple(collection, subject, predicate, object);
        } else {
            System.out.println("Triple updated successfully!");
        }

        //write to log file after update
        // Get current timestamp
        long timestamp = System.currentTimeMillis();

        // Format the update entry
        String updateEntry = String.format("%d,%s,%s,%s,%d\n", sequence_no, subject, predicate, object, timestamp);
        System.out.println(updateEntry);
        sequence_no++;

        // Write the update entry to a file (replace with your actual file path)
        try (FileWriter writer = new FileWriter("/home/yukta/College/sem6/NoSQL/project/Project/src/server_2.txt", true)) {
            writer.append(updateEntry);
            // No need to flush here as FileWriter doesn't buffer by default
        }

    }

    public static void queryTriple(MongoCollection<Document> collection, Scanner scanner) {
        System.out.print("Enter subject to query (or leave blank for all): ");
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

    public static void merge(int serverId, Connection connection) throws IOException {

        // Replace with your actual log file paths
        String localLogFile = "server_2.txt";
        String remoteLogFile = "server_" + serverId + ".txt";

        // Merge logs
        mergeLogs(localLogFile, remoteLogFile, serverId, connection);
    }


    private static void mergeLogs(String localLogFile, String remoteLogFile, int serverId, Connection connection) throws IOException {

        BufferedReader localReader = new BufferedReader(new FileReader(localLogFile));
        BufferedReader remoteReader = new BufferedReader(new FileReader(remoteLogFile));

        try {
            //get the lines till latest updated part
            Long[] LastSynced = lastSyncedLines.get(serverId);
            Long localLastSynced = LastSynced[0];
            Long RemoteLastSynced = LastSynced[1];

            String localLine;
            String remoteLine;
            Long currentLocalseq = 0L;
            Long currentRemoteseq = 0L;

            while ((localLine = localReader.readLine()) != null && currentLocalseq < localLastSynced) {
                String[] parts = localLine.split(",");
                currentLocalseq = Long.parseLong(parts[0]);
            }

            while ((remoteLine = remoteReader.readLine()) != null && currentRemoteseq < RemoteLastSynced) {
                String[] parts = remoteLine.split(",");
                currentRemoteseq = Long.parseLong(parts[0]);
            }

            Map<String, String[]> latestUpdates = new HashMap<>(); // Track latest updates (subject, predicate) -> object

            while ((localLine = localReader.readLine()) != null) {

                String[] localParts = localLine.split(",");

                currentLocalseq = Long.parseLong(localParts[0]);
                String localSubject = localParts[1];
                String localPredicate = localParts[2];
                String localObject = localParts[3];
                String localTimestamp = localParts[4]; // Assuming timestamp is the 4th element

                String key = localSubject + "," + localPredicate;

                // Apply last write wins for (subject, predicate) pair based on timestamps
                String[] latestValue = latestUpdates.get(key);

                if (latestValue == null) {

                    String[] tmp = {localObject, localTimestamp};
                    latestUpdates.put(key, tmp);
                    continue;
                }
                if (isNewerLine(localTimestamp, latestValue[1])) {
                    //processLogEntry(remoteLine);
                    latestValue[0] = localObject;
                    latestUpdates.put(key, latestValue);
                }
            }

            while ((remoteLine = remoteReader.readLine()) != null) {
                String[] remoteParts = remoteLine.split(",");

                currentRemoteseq = Long.parseLong(remoteParts[0]);
                String remoteSubject = remoteParts[1];
                String remotePredicate = remoteParts[2];
                String remoteObject = remoteParts[3];
                String remoteTimestamp = remoteParts[4]; // Assuming timestamp is the 4th element

                String key = remoteSubject + "," + remotePredicate;

                // Apply last write wins for (subject, predicate) pair based on timestamps
                String[] latestValue = latestUpdates.get(key);

                if (latestValue == null) {

                    String[] tmp = {remoteObject, remoteTimestamp};
                    latestUpdates.put(key, tmp);
                    continue;
                }
                if (isNewerLine(remoteTimestamp, latestValue[1])) {
                    //processLogEntry(remoteLine);
                    latestValue[0] = remoteObject;
                    latestUpdates.put(key, latestValue);
                }
            }

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

                // Call updateTriple with extracted subject, predicate, and object
                updateTriple(collection, subject, predicate, object);
            }

            //Update local last synced line after successful merge
            //Long[] LastSyncedUpdated = new Long[] {currentLocalseq, currentRemoteseq};
            //lastSyncedLines.put(serverId, LastSyncedUpdated);

        } finally {
            localReader.close();
            remoteReader.close();
        }
    }

    private static boolean isNewerLine(String timestamp1, String timestamp2) {
        // Try parsing timestamps
        LocalDateTime parsedTimestamp1 = parseTimestamp(timestamp1);
        LocalDateTime parsedTimestamp2 = parseTimestamp(timestamp2);

        // Handle missing timestamps (consider one newer if other is present)
        if (parsedTimestamp1 == null) {
            return parsedTimestamp2 != null;
        } else if (parsedTimestamp2 == null) {
            return true;
        }

        // Compare parsed timestamps
        return parsedTimestamp1.isAfter(parsedTimestamp2);
    }

    private static LocalDateTime parseTimestamp(String timestamp) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
            return LocalDateTime.parse(timestamp, formatter);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    public MongoCollection<Document> getCollection() {
        return this.collection;
    }

    public void closeConnection(){
        this.mongoClient.close();
    }
}

