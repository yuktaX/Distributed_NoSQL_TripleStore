import java.io.*;
import java.sql.*;
import java.util.*;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Postgres {

    private static final String DATABASE_URL = "jdbc:postgresql://localhost:5432/NOSQL_project";
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "postgres";
    private static final int ID = 1;
    static long sequence_no = 1;

    public Postgres() {}

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
            System.out.println(resultSet);
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
        try (FileWriter writer = new FileWriter( Main.path + "server_1.txt", true)) {
            writer.append(updateEntry);
        }

    }

    public static Map<String, String []> queryTriple(Connection connection, String subject) throws SQLException {
        String sql = "SELECT * FROM sample_yago";
        if (subject.length() != 0) {
            sql += " WHERE subject = ?";
        } else {
            System.out.println("No subject entered");
            return null;
        }

        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setString(1, subject);

        ResultSet resultSet = statement.executeQuery();

        Map<String, String []> results = new HashMap<>();
        while (resultSet.next()) {
            String retrievedSubject = resultSet.getString("subject");
            String retrievedPredicate = resultSet.getString("predicate");
            String retrievedObject = resultSet.getString("object");
            // System.out.println("Subject: " + retrievedSubject + ", Predicate: " + retrievedPredicate + ", Object: " + retrievedObject);
            String[] tmp = {retrievedPredicate, retrievedObject};
            results.put(retrievedSubject,tmp);
        }
        resultSet.close();
        statement.close();

        return results;
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

            //get the lines till latest updated part
            Long[] LastSynced = Main.lastSyncedGlobal.get(ID).get(serverId);
            Long localLastSynced = LastSynced[0];
            Long RemoteLastSynced = LastSynced[1];

            String localLine;
            String remoteLine;
            Long currentLocalseq = 0L;
            Long currentRemoteseq = 0L;

            Map<String, String[]> latestUpdates = new HashMap<>(); // Track latest updates (subject, predicate) -> object

            while ((localLine = localReader.readLine()) != null) {
                if(localLine.length() == 0)
                    continue;
                System.out.print("in local file-");
                System.out.println(localLine);

                String[] localParts = localLine.split(",");

                currentLocalseq = Long.parseLong(localParts[0]);

                if(currentLocalseq <= localLastSynced)
                    continue;

                System.out.print("in local file NEW-");
                System.out.println(localLine);

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
                if(remoteLine.length() == 0)
                    continue;
                System.out.print("in remote file-");
                System.out.println(remoteLine);
                String[] remoteParts = remoteLine.split(",");

                currentRemoteseq = Long.parseLong(remoteParts[0]);

                if (currentRemoteseq <= RemoteLastSynced)
                    continue;

                System.out.print("in remote file NEW-");
                System.out.println(remoteLine);

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
                try {
                    if (isNewerLine(remoteTimestamp, latestValue[1])) {
                        latestValue[0] = remoteObject;
                        latestUpdates.put(key, latestValue);
                    }
                }
                catch (Exception IllegalArgumentException){
                    //System.out.print("um");
                    //System.out.print(remoteTimestamp + "-" + latestValue[1]);
                }
            }

            System.out.println("------in merge------");
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

    private static boolean isNewerLine(String timestamp1str, String timestamp2str) throws IllegalArgumentException {

        if(timestamp1str.equals(timestamp2str))
            return false;

        Timestamp timestamp1 = Timestamp.valueOf(timestamp1str);
        Timestamp timestamp2 = Timestamp.valueOf(timestamp2str);

        // Compare timestamps directly
        return timestamp1.after(timestamp2);
    }
}


