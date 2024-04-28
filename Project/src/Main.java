import com.mongodb.client.MongoCollection;
import org.bson.Document;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import com.mongodb.client.MongoCollection;

public class Main {

    public static Map<Integer, Map<Integer, Long[]>> lastSyncedGlobal = new HashMap<>();
    public static String path = "/home/yukta/College/sem6/NoSQL/project/Project/src/"; //change path to log files

    public static void main(String[] args) throws SQLException, IOException {
        Scanner scanner = new Scanner(System.in);

        //total servers
        int n = 2;

        //different servers
        Postgres server_postgres = new Postgres();
        MongoDB server_mongo = new MongoDB();

        //initialize last synced logs
        for(int i = 1; i <= n; i++){
//            try {
//                FileWriter writer = new FileWriter(path + "server_" + i + ".txt");
//                writer.write("");  // Empty write to clear the file
//                writer.close();
//                //System.out.println("File cleared successfully!");
//            } catch (IOException e) {
//                System.err.println("Error clearing file: " + e.getMessage());
//            }
            Map<Integer, Long[]> locals = new HashMap<>();
            for(int j = 1; j <= n; j++){
                Long[] tmp = new Long[] {0L, 0L};
                locals.put(j, tmp);
            }
            lastSyncedGlobal.put(i, locals);
        }

        //connect to postgres
        Connection connection = server_postgres.connectToDatabase();
        //connect to mongo
        MongoCollection<Document> collection = server_mongo.getCollection();

        while(true) {

            System.out.println("\nChoose your server:");
            System.out.println("Server 1 - Postgres");
            System.out.println("Server 2 - MongoDB");
            System.out.println("Server 3 (inprogress)");
            System.out.println("4. Exit");
            System.out.print("Enter your choice: ");

            int server_choice = scanner.nextInt();

            if (server_choice == 4){
                System.out.println("Exiting...");
                scanner.close();
                break;
            }

            while (true) {
                System.out.println("\nTriple Store Menu:");
                System.out.println("1. Update");
                System.out.println("2. Query");
                System.out.println("3. Merge (Not Implemented)");
                System.out.println("4. Exit");
                System.out.print("Enter your choice: ");

                int choice = scanner.nextInt();

                if (choice == 4){
                    System.out.println("Exiting from server...");
                    break;
                }

                switch (choice) {
                    case 1:
                        System.out.print("Enter subject: ");
                        scanner.nextLine(); // Consume extra newline
                        String subject = scanner.nextLine();

                        System.out.print("Enter predicate: ");
                        String predicate = scanner.nextLine();

                        System.out.print("Enter new object value: ");
                        String object = scanner.nextLine();

                        if(server_choice == 1)
                            server_postgres.updateTriple(connection, subject, predicate, object);
                        if(server_choice == 2)
                            server_mongo.updateTriple(collection, subject, predicate, object);

                        break;
                    case 2:

                        if(server_choice == 1)
                            server_postgres.queryTriple(connection, scanner);
                        if(server_choice == 2)
                            server_mongo.queryTriple(collection, scanner);

                        break;

                    case 3:
                        System.out.print("Enter server to merge with: ");
                        int server_id = scanner.nextInt();

                        if(server_choice == 1)
                            server_postgres.merge(server_id, connection);
                            //server_mongo.merge()

//                        if(server_choice == 2)
//                            server_mongo.queryTriple(collection, scanner);

                        updateLastSyncedLine(server_choice, server_id);
                        printMap(lastSyncedGlobal);
                        break;

                    default:
                        System.out.println("Invalid choice!");
                }
            }
        }

    }

    //syncs latest updated line after merge
    private static void updateLastSyncedLine(int serverId1, int serverId2) throws IOException {

        BufferedReader reader1 = new BufferedReader(new FileReader(path + "server_" + serverId1 + ".txt"));
        BufferedReader reader2 = new BufferedReader(new FileReader(path + "server_" + serverId2 + ".txt"));

        try {
            String line;
            Long[] lastest = lastSyncedGlobal.get(serverId1).get(serverId2);
            Long currentmyLine = lastest[0]; //server 1
            Long currentyourLine = lastest[1]; //server 2

            while ((line = reader1.readLine()) != null) {
                if(line.length() == 0)
                    continue;
                String[] parts = line.split(",");
                Long myLine = Long.parseLong(parts[0]);
                if (currentmyLine < myLine) {
                    currentmyLine = myLine;
                }
            }

            while ((line = reader2.readLine()) != null) {
                if(line.length() == 0)
                    continue;
                String[] parts = line.split(",");
                Long yourLine = Long.parseLong(parts[0]);
                if (currentyourLine < yourLine) {
                    currentyourLine = yourLine;
                }
            }

            lastSyncedGlobal.get(serverId1).put(serverId2, new Long[]{currentmyLine, currentyourLine});
            lastSyncedGlobal.get(serverId2).put(serverId1, new Long[]{currentyourLine, currentmyLine});

        } finally {
            reader1.close();
            reader2.close();
        }
    }

    //testing func for printing sync map
    public static void printMap(Map<Integer, Map<Integer, Long[]>> map) {
        if (map == null || map.isEmpty()) {
            System.out.println("Map is empty.");
            return;
        }

        for (Map.Entry<Integer, Map<Integer, Long[]>> outerEntry : map.entrySet()) {
            int outerKey = outerEntry.getKey();
            Map<Integer, Long[]> innerMap = outerEntry.getValue();

            System.out.println("Outer Key: " + outerKey);

            if (innerMap == null || innerMap.isEmpty()) {
                System.out.println("  Inner Map is empty.");
                continue;
            }

            for (Map.Entry<Integer, Long[]> innerEntry : innerMap.entrySet()) {
                int innerKey = innerEntry.getKey();
                Long[] innerValue = innerEntry.getValue();

                System.out.println("    Inner Key: " + innerKey);
                System.out.print("      Inner Value: [");

                if (innerValue != null && innerValue.length > 0) {
                    for (int i = 0; i < innerValue.length; i++) {
                        System.out.print(innerValue[i] + (i == innerValue.length - 1 ? "" : ", "));
                    }
                } else {
                    System.out.print("null");
                }
                System.out.println("]");
            }
        }
    }

}
