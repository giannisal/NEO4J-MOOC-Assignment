import pandas as pd
import sys
import csv
from neo4j import GraphDatabase

graphdb = GraphDatabase.driver(uri="neo4j://localhost:7687", auth=("neo4j", "9599"))
session = graphdb.session()

query_string0 = "MATCH(n) DETACH DELETE n"
query_string1 = '''USING PERIODIC COMMIT 500 LOAD CSV WITH HEADERS FROM 'file:///user4j.csv' AS line FIELDTERMINATOR ','
CREATE (:USER {ID: line.USERID})'''
query_string2 = '''USING PERIODIC COMMIT 500 LOAD CSV WITH HEADERS FROM 'file:///target4j.csv' AS line FIELDTERMINATOR ','
CREATE (t:TARGET {ID : line.TARGETID})'''
query_string3 = '''USING PERIODIC COMMIT 500 LOAD CSV WITH HEADERS FROM 'file:///zero4j.csv' AS line FIELDTERMINATOR ','
MATCH (u:USER { ID: line.USERID }),(t:TARGET { ID: line.TARGETID })
MERGE (u)-[r:TOOK {ID : line.ACTIONID, f0 : line.FEATURE0, f1 : line.FEATURE1, f2 : line.FEATURE2, f3 : line.FEATURE3, tim : line.TIMESTAMP}]->(t)'''
query_string4 = '''USING PERIODIC COMMIT 500 LOAD CSV WITH HEADERS FROM 'file:///one4j.csv' AS line FIELDTERMINATOR ','
MATCH (u:USER { ID: line.USERID }),(t:TARGET { ID: line.TARGETID })
MERGE (u)-[r:DROPPED {ID : line.ACTIONID, f0 : line.FEATURE0, f1 : line.FEATURE1, f2 : line.FEATURE2, f3 : line.FEATURE3, tim : line.TIMESTAMP}]->(t)'''
#session.run(query_string0)
session.run(query_string1)
session.run(query_string2)
session.run(query_string3)
session.run(query_string4)
#print(node1)
