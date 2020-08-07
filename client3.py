import rpyc
import os
import pickle
import pandas as pd
import random
import threading
from multiprocessing import Pool

nodes={}
nodes_file_location=os.path.expanduser('~')+'/Desktop/nodes_file.txt'
with open(nodes_file_location,'r') as f:
	vals=f.read()
minions=vals.split('\n')
infiles=[]
print(minions)
for m in minions:
	port,ip=m.split(':')
	nodes[port]=ip	
	# nodes[port]=ip
# 	print(m)
	
# print(nodes)

def read_data(input_file):
	data=pd.read_csv(input_file)
	
	data_category_range=data['County'].unique()
	data_category_range = data_category_range.tolist()
	folder_name=''.join(input_file.split('/')[-1].split('.')[:-1])
	folder_name=str(folder_name)
	for i,value in enumerate(data_category_range):
		# port=int(random.choice(list(nodes.keys())))
		# ip='localhost'
		# c=rpyc.connect(ip,port=port)
		content=pickle.dumps(data[data['County'] == value])
		file_name=value
		infiles.append([content,folder_name,file_name])
		
		# c.root.write_data(content,folder_name,file_name)
def upload_file(file):
	content=file[0]
	folder_name=file[1]
	file_name=file[2]
	port=int(random.choice(list(nodes.keys())))
	ip='localhost'
	c=rpyc.connect(ip,port=port)
	c.root.write_data(content,folder_name,file_name)

k=input('Enter file location\n')
read_data(k)
pool=Pool(len(nodes.keys()))
pool.map(upload_file,infiles)