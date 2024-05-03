package testcases;
import org.bson.Document;
import org.junit.Test;

import com.mongodb.client.MongoCollection;

import maincode.MongoDB;
import maincode.Postgres;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import java.io.FileWriter;
import java.io.IOException;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;

public class TestCleanUp {
    @Test
    public void deletePostGres() throws SQLException{
        Postgres server_postgres= new Postgres();
        Connection connection = server_postgres.connectToDatabase();

        assertNotNull(connection);
        String subject="<subject1>";
        String sql = "DELETE FROM sample_yago";

        assertNotSame(0, subject.length());
        sql += " WHERE subject = ?";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setString(1, subject);
        int rowsDeleted = statement.executeUpdate();
        assertEquals(1, rowsDeleted);
        statement.close();
    }

    @Test
    public void deleteMongo() throws SQLException{
        MongoDB server_mongo= new MongoDB();
        MongoCollection<Document> collection = server_mongo.getCollection();
        assertNotNull(collection);
        String subjectToDelete = "<subject1>";
        Document filter = new Document("subject", subjectToDelete);
        DeleteResult result = collection.deleteMany(filter);
        assertEquals(1, result.getDeletedCount());
    }

    @Test
    public void clearLog(){
        String basePath = "/mnt/c/Users/kalya/OneDrive/Desktop/NOSQL/Final_Project/Distributed_NoSQL_TripleStore/Project/Nosql Project/src/testcases/"; // Replace with the actual file path
        String filePath = basePath + "server_1.txt";
        String filePath2=basePath + "server_2.txt";
        try {
            // Open the file for writing (this will clear its content)
            FileWriter fileWriter = new FileWriter(filePath);

            // Close the FileWriter
            fileWriter.close();

            FileWriter fileWriter2 = new FileWriter(filePath2);

            // Close the FileWriter
            fileWriter2.close();
            System.out.println("File cleared successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while clearing the file.");
            e.printStackTrace();
        }
    }
}