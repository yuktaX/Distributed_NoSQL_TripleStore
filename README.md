# NoSQL-Project
Final project for CS 839 NoSQL Systems course

## Setting up Databases

All the below steps assume that you have the softwares installed in your system, if not then do so before going ahead.

### Postgres
- Create a new DB and follow the steps mentioned in `postgres.sql`
- Load the data from `test.csv` into the table


### MongoDB
- Create a new DB and load the `test.csv` using the cmd: \
  `mongoimport --db my_database --collection customers --file [path to file] --headerline --type csv`
- In this DB, create a new collection for sequence number using the cmd: `db.current_seq.insertOne({ seq_no: 1 })`

### Neo4j
- Follow the instructions present in [neo4jsetup.txt](/neo4jsetup.txt)

## Setting up the Codebase
- Download the `Project` folder and set it up on your preferable IDE.
- Add the following dependencies based on whichever IDE and build you use (Maven/Gradle or internal builds like Intellij IDEA) 
  1. postgresql-42.7.3.jar
  2. mongodb-driver-sync-5.0.1.jar
  3. mongodb-driver-core-5.0.1.jar
  4. bson-record-codec-5.0.1.jar
  5. bson-5.0.1.jar
  6. neo4j-java-driver-4.4.2.jar
  7. reactive-streams-1.0.4.jar
  8. hamcrest-core.jar
  9. junit4.jar

- Change all the file paths based on your local file system
- Change the DB connection parameters as per your DB name, username and passwords
- Run the `Main()` function. A terminal menu will appear, query as you choose. You can now interact with the triple store.

## Indexing 

This project creates the distributed triple store for the yago dataset which has 13M rows. So to ensure efficient query and update times, we have performed indexing on each database based on the subject key.

### Postgres

To perfrom indexing on Postgres checkout [postgresIndexing](/postgresIndex.txt)

### MongoDB

To perform indexing on MongoDB checkout [mongoDbIndexing](/mongoIndexing.txt)

### Neo4j

To perform indexing on Neo4j, checkout [neo4jIndexing](/neo4jIndexing.txt)



