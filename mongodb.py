# mongodb.py
import sys # for handling CLI input
import csv # for reading and parsing TSV file
from pymongo import MongoClient 

# connect to local MongoDB instance
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
    doc = collection.find_one({"id":node_id}) #finds document by id
    return doc["name"] if doc else node_id

def load_nodes():
    """Load nodes.tsv into MongoDB"""
    
    #open tsv file containing node data
    with open("data/nodes.tsv", "r") as f:
        reader = csv.DictReader(f, delimiter="\t") #reads tsv as dictionary rows , each row is a dictionary
        
        #iterates through each row in file
        for row in reader: 
            #create a document to insert into mongoDB
            doc = {
                "id" : row["id"],
                "name" : row["name"],
                "kind" : row["kind"],
                #initializes relationship fields as empty lists (only for disease)
                "TREATS" : [], #store name of compounds that treats this disease
                "PALLIATES" : [], #compounds that palliates this disease
                "ASSOCIATES" :[], # genes associated with this disease
                "LOCALIZES" : [] # anatomy locations of this disease 
            }
            collection.insert_one(doc) #insert document into mongoDB
    print("Nodes loaded")
    
def load_edges():
    """ 
    loads edges.tsv and transforms raw edge data into an aggregated format
    stored directly inside mongoDB doc
    
    RAW DATA FORMAT (edges.tsv):
        source | metaedge | target
        
    STRATEGY:
        instead of storing each edge as a separate record, we pre-aggregate relationships into its corresponding doc
        - longer load time: process each edge types we need and update disease doc 
        - shorter query time: perform only one lookup
    """
    
    with open("data/edges.tsv", "r") as f:
        reader = csv.DictReader(f, delimiter="\t")
        
        # Add each metaedge (relationship)
        for row in reader: 
            source = row["source"] # ID of source node
            metaedge = row["metaedge"] #type of relationship ("CtD", "DaG", etc)
            target = row["target"] # ID of target node 
            
            # CtD: Compound treats Disease
            if metaedge == "CtD" :
                c_name = get_name(source) #get compound name from ID
                # find disease doc and add compound to TREATS list in disease doc
                collection.update_one({"id": target}, {"$push": {"TREATS": c_name}})
            
            # Compound palliates Disease: store compound name in disease doc , under "PALLIATES"
            elif metaedge == "CpD" :
                c_name = get_name(source)
                collection.update_one({"id": target}, {"$push": {"PALLIATES": c_name}})
            
            # Disease associates Gene aka gene cause disease: store gene name in disease doc , under "ASSOCIATES"
            elif metaedge == "DaG" :
                g_name = get_name(target)
                collection.update_one({"id": source}, {"$push": {"ASSOCIATES": g_name}})
            
            # Disease localises Anatomy aka disease occurs in this location of body: store anatomy name in disease doc , under "LOCALIZES"
            elif metaedge == "DlA" :
                a_name = get_name(target)
                collection.update_one({"id": source}, {"$push": {"LOCALIZES": a_name}})
    
    print("Edges loaded")

# query a disease and print its relationships    
def query1(disease_id):
    # retrieve disease document by its ID
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