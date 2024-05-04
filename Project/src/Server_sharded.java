import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

public abstract class Server_sharded {

    static String logFilePath = "/home/yukta/College/sem6/NoSQL/project/NoSQL-Project/Project/src/Logs/server_";

    protected static Map<String, String[]> mergeLogs(int ID, int remoteID) throws IOException, ParseException {

        // Track the last synced line number for local and remote files
        Long[] lastSynced = Main.lastSyncedGlobal.get(ID).get(remoteID);
        Long localLastSynced = lastSynced[0];
        Long remoteLastSynced = lastSynced[1];

        String localLogFile = logFilePath + ID;
        String remoteLogFile = logFilePath + remoteID;

        Long currentLocalseq = 0L;
        Long currentRemoteseq = 0L;

        // Find the appropriate local log file and line number to start reading from
        int localFileIndex = (int) (localLastSynced / 100);
        int localLineOffset = (int) (localLastSynced % 100);

        // Find the appropriate remote log file and line number to start reading from
        int remoteFileIndex = (int) (remoteLastSynced / 100);
        int remoteLineOffset = (int) (remoteLastSynced % 100);

        Map<String, String[]> latestUpdates = new HashMap<>();

        int localLineCount = 0;
        int remoteLineCount = 0;

        while (true) {
            // Read from local log files
            BufferedReader localReader = new BufferedReader(new FileReader(localLogFile + "_" + String.valueOf(localFileIndex)));

            String localLine;
            int stopper_flg = 0;

            while (true) {

                if ((localLine = localReader.readLine()) == null) {
                    stopper_flg = 1;
                    break;
                }

                if (localLine.length() == 0)
                    continue;
                localLineCount++;

                if (localLineCount < localLineOffset) continue;


                // Process the line
                // Your existing logic for processing the line goes here
                String[] localParts = localLine.split(",");
                currentLocalseq = Long.parseLong(localParts[0]);

                System.out.print("in local file NEW-");
                System.out.println(localLine);

                String localSubject = localParts[1];
                String localPredicate = localParts[2];
                String localObject = localParts[3];
                String localTimestamp = localParts[4];

                String key = localSubject + "," + localPredicate;

                // Apply last write wins for (subject, predicate) pair based on timestamps
                String[] latestValue = latestUpdates.get(key);

                if (latestValue == null) {
                    String[] tmp = {localObject, localTimestamp, "0"};
                    latestUpdates.put(key, tmp);
                    continue;
                }
                if (isNewerLine(localTimestamp, latestValue[1])) {
                    latestValue[0] = localObject;
                    latestValue[1] = localTimestamp;
                    latestUpdates.put(key, latestValue);
                }

                // If processed 100 lines, move to the next local log file
                if (localLineCount % 100 == 0) {
                    localFileIndex++;
                    localLineCount = 0;
                    localLineOffset = 0;
                    break;
                }
            }
            if(stopper_flg == 1)    break;

            localReader.close();
        }

        while (true) {
            // Read from remote log files
            BufferedReader remoteReader = new BufferedReader(new FileReader(remoteLogFile + "_" + String.valueOf(remoteFileIndex)));

            String remoteLine;
            int stopper_flg = 0;

            while (true) {

                if ((remoteLine = remoteReader.readLine()) == null) {
                    stopper_flg = 1;
                    break;
                }

                if (remoteLine.length() == 0)
                    continue;
                remoteLineCount++;

                if (remoteLineCount < remoteLineOffset) continue;


                // Process the line
                // Your existing logic for processing the line goes here
                String[] remoteParts = remoteLine.split(",");
                currentLocalseq = Long.parseLong(remoteParts[0]);

                System.out.print("in remote file NEW-");
                System.out.println(remoteLine);

                String remoteSubject = remoteParts[1];
                String remotePredicate = remoteParts[2];
                String remoteObject = remoteParts[3];
                String remoteTimestamp = remoteParts[4];

                String key = remoteSubject + "," + remotePredicate;

                // Apply last write wins for (subject, predicate) pair based on timestamps
                String[] latestValue = latestUpdates.get(key);

                if (latestValue == null) {
                    String[] tmp = {remoteObject, remoteTimestamp, "1"};
                    latestUpdates.put(key, tmp);
                    continue;
                }
                if (isNewerLine(remoteTimestamp, latestValue[1])) {
                    latestValue[0] = remoteObject;
                    latestValue[1] = remoteTimestamp;
                    latestValue[2] = "1";
                    latestUpdates.put(key, latestValue);
                }

                // If processed 100 lines, move to the next remote log file
                if (remoteLineCount % 100 == 0) {
                    remoteFileIndex++;
                    remoteLineCount = 0;
                    remoteLineOffset = 0;
                    break;
                }
            }
            if(stopper_flg == 1)    break;

            remoteReader.close();
        }

        return latestUpdates;
    }


    protected static boolean isNewerLine(String timestamp1str, String timestamp2str) throws ParseException {

        if (timestamp1str.equals(timestamp2str))
            return false;

        long t1 = Long.parseLong(timestamp1str);
        long t2 = Long.parseLong(timestamp2str);

        // Compare timestamps directly
        return t1 >= t2;
    }

    protected static void writeLog(int ID, long sequence_no, String subject, String predicate, String object, String time) throws IOException {
        //write to log file after update, get current timestamp if not a merge write
        String fileToWrite = logFilePath + ID + "_" + (int) sequence_no / 100;

        long timestamp;
        if (time.length() == 0)
            timestamp = System.currentTimeMillis();
        else
            timestamp = Long.parseLong(time);

        // Format the update entry
        String updateEntry = String.format("%d,%s,%s,%s,%d\n", sequence_no, subject, predicate, object, timestamp);
        System.out.println(updateEntry);

        // Write the update entry to a file (replace with your actual file path)
        try (FileWriter writer = new FileWriter( fileToWrite, true)) {
            writer.append(updateEntry);
        }
    }
}
