# NoSQL-Project
Final project for NoSQL Systems course

logic:
1. Each line in the log file will look like (sequence_no, subject, predicate, object, timestamp).
2. Each merge function will have a static copy of the latest sequence number read from the logs of other servers, so on calling merge it should merge from last synced line.
3. First, iterate through both log files to reach the latest synced position.
4. Next. Read both log files and for each (subject, predicate) pair, look at the latest object that's updated and update the map with (object, timestamp) value.
5. After iterating thru both logs, the local map will have all the (subject, predicate) pairs with latest write. Update accordingly in both servers.
6. After this, update the last synced lines.

To do:
lot of testing with lot of cases.
1. The merge for server 2 has to be tested, code hasnt been added yet for this internal call.



