package testcases;
import org.bson.Document;
import org.junit.Test;

import com.mongodb.client.MongoCollection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import maincode.*;
import java.io.FileWriter;
import java.io.IOException;

public class MainTest {

    @Test
    public void testPostGresConnect() throws SQLException{
        Postgres server_postgres= new Postgres();
        Connection connection = server_postgres.connectToDatabase();

        assertNotNull(connection);
    }

    @Test
    public void testMongoConnect() throws SQLException{
        MongoDB server_mongo= new MongoDB();
        MongoCollection<Document> collection = server_mongo.getCollection();

        assertNotNull(collection);
    }


    @Test
    public void insertPostGres() throws SQLException, IOException{
        Postgres server_postgres= new Postgres();
        Connection connection = server_postgres.connectToDatabase();
        String subject = "<subject1>";
        String predicate = "<predicate1>";
        String object = "<object1>";

        server_postgres.updateTriple(connection, subject, predicate, object);

        Map<String,String[]> results=server_postgres.queryTriple(connection, subject);

        assertEquals(1, results.size());
        assertEquals(object,results.get(subject)[1]);
    }


    @Test
    public void QueryPostGres() throws SQLException, IOException{
        Postgres server_postgres= new Postgres();
        Connection connection = server_postgres.connectToDatabase();
        String subject = "<subject1>";

        Map<String,String[]> results=server_postgres.queryTriple(connection, subject);

        assertEquals(1, results.size());
        assertEquals("<predicate1>", results.get(subject)[0]);
        assertEquals("<object1>",results.get(subject)[1]);
    }

    @Test
    public void updatePostGres() throws SQLException, IOException{
        Postgres server_postgres= new Postgres();
        Connection connection = server_postgres.connectToDatabase();
        String subject = "<subject1>";
        String predicate = "<predicate1>";
        String object = "<object1_updated>";

        server_postgres.updateTriple(connection, subject, predicate, object);

        Map<String,String[]> results=server_postgres.queryTriple(connection, subject);

        assertEquals(1, results.size());
        assertEquals(object,results.get(subject)[1]);
    }


    @Test
    public void insertMongoDB() throws SQLException, IOException{
        MongoDB server_mongo= new MongoDB();
        MongoCollection<Document> collection = server_mongo.getCollection();
        String subject = "<subject1>";
        String predicate = "<predicate1>";
        String object = "<object2>";

        server_mongo.updateTriple(collection, subject, predicate, object);

        Map<String,String[]> results=server_mongo.queryTriple(collection, subject);

        assertEquals(1, results.size());
        assertEquals(object,results.get(subject)[1]);
    }

    @Test
    public void queryMongoDB() throws SQLException, IOException{
        MongoDB server_mongo= new MongoDB();
        MongoCollection<Document> collection = server_mongo.getCollection();
        String subject = "<subject1>";

        Map<String,String[]> results=server_mongo.queryTriple(collection, subject);

        assertEquals(1, results.size());
        assertEquals("<object2>",results.get(subject)[1]);
    }

    @Test
    public void updateMongoDB() throws SQLException, IOException{
        MongoDB server_mongo= new MongoDB();
        MongoCollection<Document> collection = server_mongo.getCollection();
        String subject = "<subject1>";
        String predicate = "<predicate1>";
        String object = "<object2_updated>";

        server_mongo.updateTriple(collection, subject, predicate, object);

        Map<String,String[]> results=server_mongo.queryTriple(collection, subject);

        assertEquals(1, results.size());
        assertEquals(object,results.get(subject)[1]);
    }



    // @Test
    // public void testMerge() throws SQLException, IOException{
    //     Postgres server_postgres= new Postgres();
    //     Connection connection = server_postgres.connectToDatabase();

    //     MongoDB server_mongo= new MongoDB();
    //     MongoCollection<Document> collection = server_mongo.getCollection();

    //     assertNotNull(collection);
    //     assertNotNull(connection);

    //     server_mongo.merge(1, collection);
    //     System.out.print("-------MERGE ONE DONE-------");
    //     server_postgres.merge(2, connection);
    //     System.out.print("-------MERGE TWO DONE-------");

    // }

}