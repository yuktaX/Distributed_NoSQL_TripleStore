import java.io.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Postgres {

    private static final String DATABASE_URL = "jdbc:postgresql://localhost:5432/NoSQL_project";
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "postgres";
    private static final int ID = 1;
    static long sequence_no = 1;

    public Postgres() {

    }

    public static Connection connectToDatabase() throws SQLException {
        System.out.println("Connecting to database...");
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("Error loading PostgreSQL driver: " + e.getMessage());
            throw new SQLException("Failed to connect to database");
        }
        return DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
    }

    public static void updateTriple(Connection connection, String subject, String predicate, String object) throws SQLException, IOException {

        //check if already present, then dont update or write to file
        String check = "SELECT * FROM sample_yago WHERE subject = ? AND predicate = ? AND object = ?";
        PreparedStatement check_statement = connection.prepareStatement(check);
        check_statement.setString(1, subject);
        check_statement.setString(2, predicate);
        check_statement.setString(3, object);
        ResultSet resultSet = check_statement.executeQuery();

        if(resultSet.next()){
            System.out.println("Object already present");
            return;
        }

        String sql = "UPDATE sample_yago SET object = ? WHERE subject = ? AND predicate = ?";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setString(1, object);
        statement.setString(2, subject);
        statement.setString(3, predicate);

        int rowsUpdated = statement.executeUpdate();
        if (rowsUpdated > 0) {
            System.out.println("Triple updated successfully!");
        } else {
            System.out.println("No triple found for update. Creating a new one.");
            insertTriple(connection, subject, predicate, object);
        }
        statement.close();

        //write to log file after update
        // Get current timestamp
        long timestamp = System.currentTimeMillis();

        // Format the update entry
        String updateEntry = String.format("%d,%s,%s,%s,%d\n", sequence_no, subject, predicate, object, timestamp);
        System.out.println(updateEntry);
        sequence_no++;

        // Write the update entry to a file (replace with your actual file path)
        try (FileWriter writer = new FileWriter("/home/yukta/College/sem6/NoSQL/project/Project/src/server_1.txt", true)) {
            writer.append(updateEntry);
            // No need to flush here as FileWriter doesn't buffer by default
        }

    }

    public static void queryTriple(Connection connection, Scanner scanner) throws SQLException {
        System.out.print("Enter subject to query (or leave blank for all): ");
        scanner.nextLine();
        String subject = scanner.nextLine(); // Consume extra newline

        String sql = "SELECT * FROM sample_yago";
        if (subject.length() != 0) {
            sql += " WHERE subject = ?";
        } else {
            System.out.println("No subject entered");
            return;
        }

        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setString(1, subject);

        ResultSet resultSet = statement.executeQuery();

        while (resultSet.next()) {
            String retrievedSubject = resultSet.getString("subject");
            String retrievedPredicate = resultSet.getString("predicate");
            String retrievedObject = resultSet.getString("object");
            System.out.println("Subject: " + retrievedSubject + ", Predicate: " + retrievedPredicate + ", Object: " + retrievedObject);
        }
        resultSet.close();
        statement.close();
    }

    public static void insertTriple(Connection connection, String subject, String predicate, String object) throws SQLException {
        String sql = "INSERT INTO sample_yago (subject, predicate, object) VALUES (?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setString(1, subject);
        statement.setString(2, predicate);
        statement.setString(3, object);
        int rowsAffected = statement.executeUpdate();
        if (rowsAffected > 0) {
            System.out.println("Successfully inserted!");
        }
        statement.close();
    }

    public static void merge(int serverId, Connection connection) throws IOException {

        // Replace with your actual log file paths
        String localLogFile = Main.path + "/server_1.txt";
        String remoteLogFile = Main.path + "/server_" + serverId + ".txt";

        // Merge logs
        mergeLogs(localLogFile, remoteLogFile, serverId, connection);
    }

    private static void mergeLogs(String localLogFile, String remoteLogFile, int serverId, Connection connection) throws IOException {

        BufferedReader localReader = new BufferedReader(new FileReader(localLogFile));
        BufferedReader remoteReader = new BufferedReader(new FileReader(remoteLogFile));

        try {
            Main.printMap(Main.lastSyncedGlobal);

            //get the lines till latest updated part
            Long[] LastSynced = Main.lastSyncedGlobal.get(1).get(serverId);
            Long localLastSynced = LastSynced[0];
            Long RemoteLastSynced = LastSynced[1];

            String localLine;
            String remoteLine;
            Long currentLocalseq = 0L;
            Long currentRemoteseq = 0L;

            while ((localLine = localReader.readLine()) != null && currentLocalseq < localLastSynced) {
                System.out.println(localLine);
                if(localLine.length() == 0)
                    continue;
                String[] parts = localLine.split(",");
                currentLocalseq = Long.parseLong(parts[0]);
            }

            while ((remoteLine = remoteReader.readLine()) != null && currentRemoteseq < RemoteLastSynced) {
                System.out.println(remoteLine);
                if(remoteLine.length() == 0)
                    continue;
                String[] parts = remoteLine.split(",");
                currentRemoteseq = Long.parseLong(parts[0]);
            }

            Map<String, String[]> latestUpdates = new HashMap<>(); // Track latest updates (subject, predicate) -> object

            while ((localLine = localReader.readLine()) != null) {
                System.out.println("in local file");
                System.out.println(localLine);

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
                System.out.println("in remote file");
                System.out.println(remoteLine);
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

            System.out.println("in merge");
            System.out.println(latestUpdates);

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
                updateTriple(connection, subject, predicate, object);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            localReader.close();
            remoteReader.close();
        }
    }


    private static LocalDateTime parseTimestamp(String timestamp) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
            return LocalDateTime.parse(timestamp, formatter);
        } catch (DateTimeParseException e) {
            return null;
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
}

//    private static void processLogEntry(String line) {
//        // Parse log entry (subject, predicate, object) and update triple store using updateTriple()
//        String[] parts = line.split(",");
//        String subject = parts[1];
//        String predicate = parts[2];
//        String object = parts[3];
//        updateTriple(subject, predicate, object);
//    }
//
//    private static void writeToLocalLog(String line) throws IOException {
//        // Append the log line to the local log file
//        BufferedWriter writer = new BufferedWriter(new FileWriter("server.txt", true));
//        writer.append(line).append("\n");
//        writer.close();
//    }
//

//
//    private static int getServerIdFromFilename(String filename) {
//        // Extract server ID from the remote log file name
//        return Integer.parseInt(filename.split("_")[1].split("\\.")[0]);
//    }
//
//    // Replace updateTriple() with your actual implementation for updating the triple store

    //    private static Long updateLastSyncedLine(int serverId) throws IOException {
//        BufferedReader reader = new BufferedReader(new FileReader("server_" + serverId + ".txt"));
//        try {
//            String line;
//            Long lastSequence = lastSyncedLines.get(serverId);
//            while ((line = reader.readLine()) != null) {
//                String[] parts = line.split(",");
//                Long sequence = Long.parseLong(parts[0]);
//                if (sequence > lastSequence) {
//                    lastSequence = sequence;
//                }
//            }
//            return lastSequence;
//        } finally {
//            reader.close();
//        }
//    }

