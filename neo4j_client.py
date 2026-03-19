import csv
import sys
from neo4j import GraphDatabase

URI  = "neo4j+ssc://26cfead1.databases.neo4j.io"
AUTH = ("26cfead1", "iAvwuK3QZWNFIj4zks5M_dHvoB25TfCCjkVPKx-xbRI")

driver = GraphDatabase.driver(URI, auth=AUTH)

# Load nodes from TSV files into Neo4j
def load_nodes():
    with driver.session() as session:
        with open("data/nodes.tsv", "r") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader: 
                node_id = row["id"]
                name = row["name"]
                kind = row["kind"]
                session.run("MERGE (n:Node {id: $id, name: $name, kind: $kind})", id=node_id, name=name, kind=kind)
    print("Nodes loaded")

# Load edges from TSV files into Neo4j
def load_edges():
    with driver.session() as session:
        with open("data/edges.tsv", "r") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader: 
                source = row["source"]
                metaedge = row["metaedge"]
                target = row["target"]
                session.run(f"MATCH (a:Node {{id: $source}}), (b:Node {{id: $target}}) MERGE (a)-[:{metaedge}]->(b)", source=source, target=target)
    print("Edges loaded")


# Query 2
def query2():
    
