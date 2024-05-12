import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.util.*;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Postgres extends Server_sharded {

    private static final String DATABASE_URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "postgres";
    private static final int ID = 1;
    static long sequence_no;

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

        String sql = "UPDATE yago SET object = ? WHERE subject = ? AND predicate = ?";
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

        writeLog(ID, sequence_no, subject, predicate, object, time);
        sequence_no++;

        //write to log file after update, get current timestamp if not a merge write
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
//        try (FileWriter writer = new FileWriter( Main.path + "server_1.txt", true)) {
//            writer.append(updateEntry);
//        }


    }

    public static void queryTriple(Connection connection, Scanner scanner) throws SQLException {
        System.out.print("Enter subject to query: ");
        scanner.nextLine();
        String subject = scanner.nextLine(); // Consume extra newline

        String sql = "SELECT * FROM yago";
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
        String sql = "INSERT INTO yago (subject, predicate, object) VALUES (?, ?, ?)";
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

    public static void merge(int serverId, Connection connection) throws IOException, SQLException, ParseException {

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
                updateTriple(connection, subject, predicate, object, time);

        }
    }

    public static void start(Connection connection) throws SQLException {
        String sql = "SELECT * FROM current_seq";
        PreparedStatement statement = connection.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();

        while (resultSet.next()) sequence_no = resultSet.getInt("seq_no");
    }

    public static void close(Connection connection) throws SQLException {
        String sql = "DELETE FROM current_seq";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.executeUpdate();

        sql = "Insert into current_seq values(?)";
        statement = connection.prepareStatement(sql);

        //temporary reset
        sequence_no = 1;

        statement.setLong(1, sequence_no);
        statement.executeUpdate();

        connection.close();
    }
}


