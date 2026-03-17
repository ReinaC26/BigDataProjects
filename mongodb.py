# mongodb.py
import sys # for handling CLI input
import csv # for reading and parsing TSV file
from pymongo import MongoClient

# connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["hetionet"]
collection = db["nodes"]

#optional: create an index on 'id' for fast lookup
collection.create_index("id") 

def get_name(node_id):
    """
    helper function to retrieve name of a node given its 'id';
    used during edge loading to pre-aggregate disease documents
    (pre-store all related compounds, anatomy and gene in each disease document)
    """
    doc = collection.find_one({"id":node_id})
    return doc["name"] if doc else node_id

def load_nodes():
    """Load nodes.tsv into MongoDB"""
    
    with open("data/nodes.tsv", "r") as f:
        reader = csv.DictReader(f, delimiter="\t")
        
        for row in reader: 
            doc = {
                "id" : row["id"],
                "name" : row["name"],
                "kind" : row["kind"],
                "TREATS" : [], #store name of compounds that will treat this disease
                "PALLIATES" : [],
                "ASSOCIATES" :[], #store name of genes associated with this disease
                "LOCALIZES" : [] #store anatomy locations this disease affects
            }
            collection.insert_one(doc)
    print("Nodes loaded")
    
def load_edges():
    with open("data/edges.tsv", "r") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader: 
            source = row["source"]
            metaedge = row["metaedge"]
            target = row["target"]
            
            if metaedge == "CtD" :
                # Compound treats Disease: store compound name in disease doc , under "TREATS"
                c_name = get_name(source)
                collection.update_one({"id": target}, {"$push": {"TREATS": c_name}})
            
            elif metaedge == "CpD" :
                # Compound palliates Disease: store compound name in disease doc , under "PALLIATES"
                c_name = get_name(source)
                collection.update_one({"id": target}, {"$push": {"PALLIATES": c_name}})
            
            elif metaedge == "DaG" :
                # Disease associates Gene aka gene cause disease: store gene name in disease doc , under "ASSOCIATES"
                g_name = get_name(target)
                collection.update_one({"id": source}, {"$push": {"ASSOCIATES": g_name}})
            
            elif metaedge == "DlA" :
                # Disease localises Anatomy aka disease occurs in this location of body: store anatomy name in disease doc , under "LOCALIZES"
                a_name = get_name(target)
                collection.update_one({"id": source}, {"$push": {"LOCALIZES": a_name}})
    
    print("Edges loaded")
    
def query1(disease_id):
    disease = collection.find_one({"id": disease_id})
    if disease is None:
        print("Disease not found")
        return
    
    print("\nDisease: ", disease["name"])
    
    print("Drugs that TREAT this disease:")
    for name in disease.get("TREATS", []):
        print(" -", name)
    
    print("\nDrugs that PALLIATES this disease:")
    for name in disease.get("PALLIATES", []):
        print(" -", name)
    
    print("\nGenes ASSOCIATED/CAUSES this disease:")
    for name in disease.get("ASSOCIATES", []):
        print(" -", name)
    
    print("\nDisease occurs in these locations:")
    for name in disease.get("LOCALIZES", []):
        print(" -", name)
        
    
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python mongodb.py create_db")
        print("  python mongodb.py query1 <DiseaseID>")
        sys.exit()

    command = sys.argv[1]
    if command == "create_db":
        load_nodes()
        load_edges()
    elif command == "query1":
        if len(sys.argv) < 3:
            print("Please provide a disease ID")
            sys.exit()
        query1(sys.argv[2])