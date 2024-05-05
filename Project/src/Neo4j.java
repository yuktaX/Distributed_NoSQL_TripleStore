import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;

import static org.neo4j.driver.Values.parameters;


public class Neo4j extends Server_sharded{
    private static final String DATABASE_URL = "bolt://localhost:7687"; // Adjust if MongoDB runs on a different port
    private static final String USERNAME = "neo4j";
    private static final String PASSWORD = "nosql";
    private static final int ID = 3;
    static long sequence_no;    

    static Map<Integer, Long[]> lastSyncedLines = Main.lastSyncedGlobal.get(3);

    public static Driver connectToDatabase() throws SQLException {
        System.out.println("Connecting to database...");
        return GraphDatabase.driver(DATABASE_URL,AuthTokens.basic(USERNAME, PASSWORD));
    }

    public Neo4j(){}

    public static void updateTriple(Driver driver, String subject, String predicate, String object, String time) throws Exception{
        try(Session session = driver.session()){

            String query = "MATCH (s {name : $subjectValue})-[p {predicate: $predicateValue}]->(o) RETURN s, p, o";

            Result result = session.run(query,parameters("subjectValue",subject,"predicateValue",predicate));

            if(result!=null){
                if(result.list().size()==0){
                    String insert = "CREATE (:Entity {name: $subjectValue})-[:RELATION {predicate: $predicateValue}]->(:Entity {name: $objectValue})";

                    Result resultinsert = session.run(insert,parameters("subjectValue",subject,"predicateValue",predicate, "objectValue",object));

                    if(resultinsert !=null) System.out.println("Triple added successfully!");
                    else throw new Exception("Failed to update");

                }
                else {
                    String update = "MATCH (s {name : $subjectValue})-[p {predicate: $predicateValue}]->(o) WITH s,p,o SET o.name = $objectValue RETURN s, p, o";

                    Result resultnew = session.run(update,parameters("subjectValue",subject,"predicateValue",predicate, "objectValue",object));

                    if(resultnew.hasNext()) System.out.println("Triple updated successfully!");
                    else throw new Exception("Failed to update");

                }

                writeLog(ID, sequence_no, subject, predicate, object, time);
                sequence_no++;
            }
            
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
    }

    public static void queryTriple(Driver driver, Scanner scanner){

        System.out.print("Enter subject to query: ");
        scanner.nextLine();
        String subjectValue = scanner.nextLine(); // Consume extra newline

        try (Session session = driver.session()) {

            // Run Cypher query
            String query = "MATCH (s {name: $subjectValue})-[p]->(o) RETURN s, p, o";

            Result result = session.run(query,parameters("subjectValue",subjectValue));

            while(result.hasNext()){
                Record record = result.next();
                Node subject = record.get("s").asNode();
                Relationship predicate = record.get("p").asRelationship();
                Node object = record.get("o").asNode();
                
                System.out.println("Subject: " + subject.get("name") + ", Predicate: " + predicate.get("predicate") + ", Object: " + object.get("name"));
            }
        } 
        catch(Exception ex){
            ex.printStackTrace();
        }

    }

    public static void merge(int serverId, Driver driver) throws Exception{
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
                updateTriple(driver, subject, predicate, object, time);

        }
    }

    public static void start(){

    }

    public void close(){

    }
}
