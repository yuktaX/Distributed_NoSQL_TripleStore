import com.mongodb.client.MongoCollection;
import org.bson.Document;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Main {

    public static Map<Integer, Map<Integer, Long[]>> lastSyncedGlobal = new HashMap<>();
    public static String path = "/home/yukta/College/sem6/NoSQL/project/NoSQL-Project/Project/src/"; //change path to log files
    public static Map<Integer, String> servers = new HashMap<>();

    public static void main(String[] args) throws SQLException, IOException {
        Scanner scanner = new Scanner(System.in);

        //total servers
        int n = 2;

        //different servers
        Postgres server_postgres = new Postgres();
        MongoDB server_mongo = new MongoDB();

        System.out.println(isNewerLine("1714386668050", "1714386668050"));

        servers.put(1, "Postgres");
        servers.put(2, "MongoDB");

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

            Console.printServerMenu();

            int server_choice = scanner.nextInt();

            if (server_choice == 4){
                System.out.println("Exiting...");
                scanner.close();
                break;
            }

            while (true) {

                Console.printStoreMenu(server_choice);

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

                        if(server_choice == 1) {
                            server_postgres.merge(server_id, connection);
                            System.out.print("-------MERGE ONE DONE-------");
                            server_mongo.merge(1, collection);
                            System.out.print("-------MERGE TWO DONE-------");
                        }

                        if(server_choice == 2) {
                            server_mongo.merge(server_id, collection);
                            System.out.print("-------MERGE ONE DONE-------");
                            server_postgres.merge(2, connection);
                            System.out.print("-------MERGE TWO DONE-------");
                        }

                        //syncing latest line after merging;
                        updateLastSyncedLine(server_choice, server_id);
                        //printMap(lastSyncedGlobal);
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

    private static boolean isNewerLine(String timestamp1str, String timestamp2str) {
        if(timestamp1str.equals(timestamp2str))
            return false;

        Timestamp timestamp1 = Timestamp.valueOf(timestamp1str);
        Timestamp timestamp2 = Timestamp.valueOf(timestamp2str);

        // Compare timestamps directly
        return timestamp1.after(timestamp2);
    }
}
