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

## Setting up the Codebase
- Download the `Project` folder and set it up on your preferable IDE.
- Add the following dependencies based on whichever IDE and build you use (Maven/Gradle or internal builds like Intellij IDEA) 
  1. postgresql-42.7.3.jar
  2. mongodb-driver-sync-5.0.1.jar
  3. mongodb-driver-core-5.0.1.jar
  4. bson-record-codec-5.0.1.jar
  5. bson-5.0.1.jar

- Change all the file paths based on your local file system
- Change the DB connection parameters as per your DB name, username and passwords
- Run the `Main()` function. A terminal menu will appear, query as you choose. You can now interact with the triple store.





