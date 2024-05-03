package MainCode;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

public abstract class Server {

    protected static Map<String, String[]> mergeLogs(int ID, String localLogFile, String remoteLogFile, int serverId) throws IOException {

        BufferedReader localReader = new BufferedReader(new FileReader(localLogFile));
        BufferedReader remoteReader = new BufferedReader(new FileReader(remoteLogFile));

        try {
            //get the lines till latest updated part
            //Testing.printMap(Main.lastSyncedGlobal);
            Long[] LastSynced = Main.lastSyncedGlobal.get(ID).get(serverId);
            Long localLastSynced = LastSynced[0];
            Long RemoteLastSynced = LastSynced[1];

            String localLine;
            String remoteLine;
            Long currentLocalseq = 0L;
            Long currentRemoteseq = 0L;


            Map<String, String[]> latestUpdates = new HashMap<>();
            // Track latest updates (subject, predicate) -> object, timestamp, [local update or remote update (0, 1)]
            // if local update then no need to write to log as it already has the latest entry
            // write only remote updates as we dont have that

            while ((localLine = localReader.readLine()) != null) {
                if (localLine.length() == 0)
                    continue;
                System.out.print("in local file-");
                System.out.println(localLine);

                String[] localParts = localLine.split(",");

                currentLocalseq = Long.parseLong(localParts[0]);

                if (currentLocalseq <= localLastSynced)
                    continue;

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
            }

            while ((remoteLine = remoteReader.readLine()) != null) {
                if (remoteLine.length() == 0)
                    continue;
                System.out.print("in remote file-");
                System.out.println(remoteLine);
                String[] remoteParts = remoteLine.split(",");

                currentRemoteseq = Long.parseLong(remoteParts[0]);

                if (currentRemoteseq <= RemoteLastSynced)
                    continue;

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

            }

            System.out.println("------in merge------");
            Testing.printMergeMap(latestUpdates);

            return latestUpdates;

        } catch (ParseException e) {
            throw new RuntimeException(e);
        } finally {
            localReader.close();
            remoteReader.close();
        }
    }

    protected static boolean isNewerLine(String timestamp1str, String timestamp2str) throws ParseException {

        if (timestamp1str.equals(timestamp2str))
            return false;

        long t1 = Long.parseLong(timestamp1str);
        long t2 = Long.parseLong(timestamp2str);

        // Compare timestamps directly
        return t1 >= t2;
    }
}
