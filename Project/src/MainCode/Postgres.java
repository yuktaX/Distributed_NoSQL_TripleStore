package MainCode;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.Map;
import java.util.Scanner;

public class Postgres extends Server {

    private static final String DATABASE_URL = "jdbc:postgresql://localhost:5432/NoSQL_project";
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

    public static void updateTriple(Connection connection, String subject, String predicate, String object, String time) throws SQLException, IOException {

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

        //write to log file after update, get current timestamp if not a merge write
        long timestamp;
        if (time.length() == 0)
            timestamp = System.currentTimeMillis();
        else
            timestamp = Long.parseLong(time);

        // Format the update entry
        String updateEntry = String.format("%d,%s,%s,%s,%d\n", sequence_no, subject, predicate, object, timestamp);
        System.out.println(updateEntry);
        sequence_no++;

        // Write the update entry to a file (replace with your actual file path)
        try (FileWriter writer = new FileWriter( Main.path + "server_1.txt", true)) {
            writer.append(updateEntry);
        }

    }

    public static void queryTriple(Connection connection, Scanner scanner) throws SQLException {
        System.out.print("Enter subject to query: ");
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

    public static void merge(int serverId, Connection connection) throws IOException, SQLException {

        // Replace with your actual log file paths
        String localLogFile = Main.path + "/server_1.txt";
        String remoteLogFile = Main.path + "/server_" + serverId + ".txt";

        // Merge logs
        Map<String, String[]> latestUpdates = mergeLogs(ID, localLogFile, remoteLogFile, serverId);

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
                updateTriple(connection, subject, predicate, object, time);
        }
    }
}


