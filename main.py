from mongodb import load_nodes as mongodb_load_nodes, load_edges as mongodb_load_edges, query1
from neo4j_query2 import load_nodes as neo4j_load_nodes, load_edges as neo4j_load_edges, query2

if __name__ == "__main__":
    while True:
        print("\nPick an option to continue:")
        print("  1. Query 1 (Disease Lookup)")
        print("  2. Query 2 (New Treatments)")
        print("  3. Exit")
        
        option = input("\nEnter option (1 or 2 or 3): ").strip()
        
        if option == "1":
            print("\nPick an option to continue:")
            print("  1. Create database")
            print("  2. Run Query 1")
            sub_option = input("  Enter option (1 or 2): ").strip()
            if sub_option == "1":
                mongodb_load_nodes()
                mongodb_load_edges()
                print("Database ready.")
            elif sub_option == "2":
                disease_id = input("  Enter Disease ID (e.g. Disease::DOID:1324): ").strip()
                query1(disease_id)
            else:
                print("Invalid option.")

        elif option == "2":
            print("\nPick an option to continue:")
            print("  1. Create database")
            print("  2. Run Query 2")
            sub_option = input("  Enter option (1 or 2): ").strip()
            if sub_option == "1":
                neo4j_load_nodes()
                neo4j_load_edges()
                print("Database ready.")
            elif sub_option == "2":
                query2()
            else:
                print("Invalid option.")

        elif option == "3":
            print("Please rerun the program to run another query.")
            break
        
        else:
            print("Unknown option.")