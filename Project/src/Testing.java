import java.util.Map;

public class Testing {
    //testing func for printing sync map
    public static void printMap(Map<Integer, Map<Integer, Long[]>> map) {
        if (map == null || map.isEmpty()) {
            System.out.println("Map is empty.");
            return;
        }

        for (Map.Entry<Integer, Map<Integer, Long[]>> outerEntry : map.entrySet()) {
            int outerKey = outerEntry.getKey();
            Map<Integer, Long[]> innerMap = outerEntry.getValue();

            System.out.println("Outer Key: " + outerKey);

            if (innerMap == null || innerMap.isEmpty()) {
                System.out.println("  Inner Map is empty.");
                continue;
            }

            for (Map.Entry<Integer, Long[]> innerEntry : innerMap.entrySet()) {
                int innerKey = innerEntry.getKey();
                Long[] innerValue = innerEntry.getValue();

                System.out.println("    Inner Key: " + innerKey);
                System.out.print("      Inner Value: [");

                if (innerValue != null && innerValue.length > 0) {
                    for (int i = 0; i < innerValue.length; i++) {
                        System.out.print(innerValue[i] + (i == innerValue.length - 1 ? "" : ", "));
                    }
                } else {
                    System.out.print("null");
                }
                System.out.println("]");
            }
        }
    }

    //testing func to print
    public static void printMergeMap(Map<String, String[]> map) {
        if (map == null || map.isEmpty()) {
            System.out.println("Map is empty.");
            return;
        }

        for (Map.Entry<String, String[]> entry : map.entrySet()) {
            String key = entry.getKey();
            String[] values = entry.getValue();

            System.out.println("Key: " + key);

            if (values == null || values.length == 0) {
                System.out.println("  Value: (empty)");
            } else {
                System.out.print("  Value: [");
                for (int i = 0; i < values.length; i++) {
                    System.out.print(values[i] + (i == values.length - 1 ? "" : ", "));
                }
                System.out.println("]");
            }
        }
    }
}
