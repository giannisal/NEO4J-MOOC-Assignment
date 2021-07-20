import pandas as pd
import sys
import csv
from neo4j import GraphDatabase

#Reading all files, inner joining to get all data in one df, removing duplicates
#and keeping label = 1 on actions with same id and both labels
neodf = pd.read_csv ('/home/giannis/Downloads/act-mooc/mooc_actions.tsv',sep='\t')
featuresdf = pd.read_csv ('/home/giannis/Downloads/act-mooc/mooc_action_features.tsv',sep='\t')
labelsdf = pd.read_csv ('/home/giannis/Downloads/act-mooc/mooc_action_labels.tsv',sep='\t')
neodf = neodf.join(featuresdf, on="ACTIONID", how='left', rsuffix="_y")
neodf = neodf.join(labelsdf, on="ACTIONID", how='left', rsuffix="_y")
neodf = neodf.drop('ACTIONID_y', axis = 1)
neodf = neodf.sort_values(by = 'LABEL')
neodf = neodf.drop_duplicates(subset = ['ACTIONID', 'TIMESTAMP'], keep = 'last').dropna(axis=0)
zero4j = neodf.loc[neodf['LABEL'] == 0]
one4j = neodf.loc[neodf['LABEL'] == 1]
user4j = neodf['USERID'].drop_duplicates()
target4j = neodf['TARGETID'].drop_duplicates()

user4j.to_csv(path_or_buf='./user4j.csv',index=False)
target4j.to_csv(path_or_buf='./target4j.csv',index=False)
zero4j.to_csv(path_or_buf='./zero4j.csv',index=False)
one4j.to_csv(path_or_buf='./one4j.csv',index=False)
