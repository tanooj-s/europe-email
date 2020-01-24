import networkx
import names
import json
import time
from networkx.readwrite.json_graph import node_link_data
from neo4j import GraphDatabase

#rw = RandomWords()
graph = networkx.DiGraph()
node_ids = []
edges = []

print ("reading email input from input text file....")
with open('email-Eu-core.txt', 'r') as f:
	for line in f:
		node_ids.extend(line.split(" "))
		edges.append(line.split(" "))


departments = ["Department %s" % (str(i)) for i in range(42)] # create fake department names e.g. department 1, department 2 etc
print ("created fake department names")
print (departments)

# create a directory to map node ids to departments (i.e. "employees to departments")
print ("Adding employee departments from input text file...")
directory = dict()
with open('email-Eu-core-department-labels.txt','r') as f:
	for line in f:
		this_row = line.split(" ")
		this_row = [int(i.strip('\n')) for i in this_row] # convert to int, strip newline characters
		emp_id = this_row[0] # use first element of row as key (employee id)
		emp_department = departments[this_row[1]] # use second element as "department"
		directory[emp_id] = emp_department # add employee department to every employee id in a dict


edges = [[int(src), int(tgt.strip('\n'))] for [src,tgt] in edges]
node_ids = list(set([int(n.strip('\n')) for n in node_ids]))

print("adding nodes to graph....")
for node_id in node_ids:
	graph.add_node(node_id, name=names.get_full_name(), department=directory[node_id])

print("creating edges....")
for edge in edges:
	graph.add_edge(u_of_edge=edge[0], v_of_edge=edge[1])
	

# not running networkx algos right now but if we do run any algos should add that code here and add as attrbutes to nodes/edges


print("dumping graph to json file")
data = node_link_data(graph, {'source': 'from', 'target': 'to', 'link': 'edges'})
with open('email_dump.json','w') as f:
	json.dump(data, f)

with open('email_dump.json','r') as f:
	data = json.load(f)
nodes = data['nodes']
edges = data['edges']


# parameterized transaction functions for neo4j driver
def push_node_to_db(tx,node):
	tx.run("CREATE (e:EMPLOYEE {name: $name, department: $department, id: $id})", name = node['name'], department = node['department'], id = node['id'])
	print ("Pushed " + node['name'] + " to db.") 

def push_edge_to_db(tx,edge):
	tx.run("MATCH (e1: EMPLOYEE {id: $from_id}) WITH e1 "
			"MATCH (e2: EMPLOYEE {id: $to_id}) "
			"MERGE (e1)-[:EMAILED]->(e2)", from_id = edge['from'], to_id = edge['to'])



print("Connecting to Neo4j database....")
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j","password"))
print("Starting to push data to Neo4j......")

with driver.session() as session:

	start1 = time.time()
	i = 1
	for node in nodes:
		session.write_transaction(push_node_to_db, node)
		print ("%s of %s nodes" % (str(i), len(nodes)))
		i += 1
	end1 = time.time()

	start2 = time.time()
	j = 1
	for edge in edges:
		session.write_transaction(push_edge_to_db, edge)
		print ("%s of %s edges" % (str(j), len(edges)))
		j += 1
	end2 = time.time()

print ("Took " + str(end1-start1) + " seconds to push " + str(len(nodes)) + " nodes to database")
print ("Took " + str(end2-start2) + " seconds to push " + str(len(edges)) + " edges to database")





# import into neo4j using apoc