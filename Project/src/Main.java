import com.mongodb.client.MongoCollection;
import org.bson.Document;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import org.neo4j.driver.Driver;


public class Main {

    public static Map<Integer, Map<Integer, Long[]>> lastSyncedGlobal = new HashMap<>();
    //public static String path = "/home/vboxuser/Desktop/Desktop/Nosql/Distributed_NoSQL_TripleStore/Project/src/"; //change path to log files
    public static String path =  "/home/yukta/College/sem6/NoSQL/project/NoSQL-Project/Project/src";
    public static Map<Integer, String> servers = new HashMap<>();

    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);

        //total servers
        int n = 3;

        //different servers
        Postgres server_postgres = new Postgres();
        MongoDB server_mongo = new MongoDB();
        Neo4j server_neo = new Neo4j();

        servers.put(1, "Postgres");
        servers.put(2, "MongoDB");
        servers.put(3, "Neo4j");

        //initialize last synced logs
        for(int i = 1; i <= n; i++){
            Map<Integer, Long[]> locals = new HashMap<>();
            for(int j = 1; j <= n; j++){
                Long[] tmp = new Long[] {0L, 0L};
                locals.put(j, tmp);
            }
            lastSyncedGlobal.put(i, locals);
        }

        //connect to postgres
        Connection connection = server_postgres.connectToDatabase();
        server_postgres.start(connection);

        //connect to mongo
        MongoCollection<Document> collection = server_mongo.getCollection();
        server_mongo.start();

        //connect to neo4j
        Driver driver = server_neo.connectToDatabase();

        while(true) {

            Console.printServerMenu();

            int server_choice = scanner.nextInt();

            if (server_choice == 4){
                System.out.println("Exiting...");
                scanner.close();
                server_postgres.close(connection);
                server_mongo.close();
                server_neo.close();
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
                            server_postgres.updateTriple(connection, subject, predicate, object, "");
                        if(server_choice == 2)
                            server_mongo.updateTriple(collection, subject, predicate, object, "");

                        if(server_choice == 3)
                            server_neo.updateTriple(driver, subject, predicate, object, "");

                        break;

                    case 2:
                        if(server_choice == 1)
                            server_postgres.queryTriple(connection, scanner);
                        if(server_choice == 2)
                            server_mongo.queryTriple(collection, scanner);

                        if (server_choice == 3)
                            server_neo.queryTriple(driver,scanner);

                        break;

                    case 3:
                    
                        System.out.print("Enter server to merge with: ");
                        int server_id = scanner.nextInt();

                        if(server_choice == 1) {

                            server_postgres.merge(server_id, connection);
                            System.out.print("-------MERGE ONE DONE-------");

                            if(server_id==2)
                                server_mongo.merge(server_choice, collection);
                            else if(server_id==3)
                                server_neo.merge(server_choice, driver);
                            
                            System.out.print("-------MERGE TWO DONE-------");
                        }

                        if(server_choice == 2) {
                            server_mongo.merge(server_id, collection);
                            System.out.print("-------MERGE ONE DONE-------");

                            if(server_id==1)
                                server_postgres.merge(server_choice, connection);
                            else if(server_id==3)
                                server_neo.merge(server_choice, driver);
                            System.out.print("-------MERGE TWO DONE-------");
                        }

                        if (server_choice == 3){
                            server_neo.merge(server_id, driver);
                            System.out.print("-------MERGE ONE DONE-------");

                            if(server_id==1)
                                server_postgres.merge(server_choice, connection);
                            else if(server_id==2)
                                server_mongo.merge(server_choice, collection);

                            System.out.print("-------MERGE TWO DONE-------");
                        }
                        //syncing latest line after merging;
                        Server_sharded.updateLastSyncedLine(server_choice, server_id);
                        break;

                    default:
                        System.out.println("Invalid choice!");
                }
            }
        }
    }
}



