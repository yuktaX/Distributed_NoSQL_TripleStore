public class Console {

    public static void printServerMenu(){
        System.out.println("\n\t\033[1mChoose your server:\033[0m"); // Bold for title
        System.out.println("\t1. Server 1 - Postgres");
        System.out.println("\t2. Server 2 - MongoDB");
        System.out.println("\t3. Server 3 - Neo4j");
        System.out.println("\t4. Exit");
        System.out.print("\tEnter your choice: ");
    }

    public static void printStoreMenu(int server_chosen){

        System.out.println("\nTriple Store Menu: " + Main.servers.get(server_chosen));
        System.out.println("1. Update");
        System.out.println("2. Query");
        System.out.println("3. Merge");
        System.out.println("4. Exit");
        System.out.print("Enter your choice: ");

    }
}
