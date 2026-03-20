import time
import csv
import sys
from neo4j import GraphDatabase

URI  = "neo4j://127.0.0.1:7687"
AUTH = ("neo4j", "HunterCollege")

driver = GraphDatabase.driver(URI, auth=AUTH)

# Load nodes from TSV file
def load_nodes():
    start_time = time.time()
    with driver.session() as session:
        session.run(r"""
            LOAD CSV WITH HEADERS FROM 'file:///nodes.tsv' AS row FIELDTERMINATOR '\t'
            MERGE (n:Node {id: row.id, name: row.name, kind: row.kind})
        """)
    print(f"Nodes loaded in {time.time() - start_time:.2f} seconds")

# Load edges from TSV file and process in batches of 5000 rows
def load_edges():
    start_time = time.time()
    batch_size = 5000
    current_batch = []
    
    with open("data/edges.tsv", "r") as f:
        reader = csv.DictReader(f, delimiter="\t")
        
        for row in reader:
            current_batch.append(row)
            
            # When we hit the batch size, load it and clear memory
            if len(current_batch) >= batch_size:
                load_edge_batch(current_batch)
                current_batch = []
        
        # Load the remaining rows
        if current_batch:
            load_edge_batch(current_batch)
            
    print(f"Edges loaded in {time.time() - start_time:.2f} seconds")

# Helper function to load edge batches into neo4j
def load_edge_batch(batch):
    # For each batch, group the edges by metaedge
    edge_groups = {}
    for row in batch:
        edge_groups.setdefault(row["metaedge"], []).append({
            "source": row["source"],
            "target": row["target"]
        })
        
    with driver.session() as session:
        for metaedge, edges in edge_groups.items():
            session.run(f"""
                UNWIND $rows AS row
                MATCH (a:Node {{id: row.source}})
                MATCH (b:Node {{id: row.target}})
                MERGE (a)-[:`{metaedge}`]->(b)
            """, rows=edges)

# Query 2: Find all compounds that can treat a new disease.
# - case 1: where compound upregulate gene and anatomy downregulate gene
# - case 2: where compound downregulate gene and anatomy upregulate gene
def query2():
    start_time = time.time()
    cypher = """
        MATCH (c:Node {kind: 'Compound'})-[:CuG]->(g:Node {kind: 'Gene'})<-[:AdG]-(a:Node {kind: 'Anatomy'})<-[:DlA]-(d:Node {kind: 'Disease'})
        WHERE NOT EXISTS { (c)-[:CtD]->(d) }
          AND NOT EXISTS { (c)-[:CpD]->(d) }
        RETURN DISTINCT c.name AS compound, d.name AS disease, 'Compound upregulates/Anatomy downregulates' AS case
 
        UNION
 
        MATCH (c:Node {kind: 'Compound'})-[:CdG]->(g:Node {kind: 'Gene'})<-[:AuG]-(a:Node {kind: 'Anatomy'})<-[:DlA]-(d:Node {kind: 'Disease'})
        WHERE NOT EXISTS { (c)-[:CtD]->(d) }
          AND NOT EXISTS { (c)-[:CpD]->(d) }
        RETURN DISTINCT c.name AS compound, d.name AS disease, 'Compound downregulates/Anatomy upregulates' AS case
 
        ORDER BY compound
    """
    with driver.session() as session:
        result = session.run(cypher)
        for record in result:
            print(f"Compound: {record['compound']}, Disease: {record['disease']}, Case: {record['case']}")
    
    print(f"Query 2 completed in {time.time() - start_time:.2f} seconds")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage:")
        print("  python3 neo4j_query2.py load_data")
        print("  python3 neo4j_query2.py query2")
        sys.exit(1)
    command = sys.argv[1]
    if command == "load_data":
        load_nodes()
        load_edges()
    elif command == "query2":
        query2()

    driver.close()
