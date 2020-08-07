import rpyc
import os
import pickle
import pandas as pd
import random
import threading
from multiprocessing import Pool
import sys



ips=[]
ports=[]
infiles=[]
# nodes={}


nodes_file_location=os.path.expanduser('~')+'/Desktop/nodes_file.txt'
file_table_location=os.path.expanduser('~/Desktop/file_table_store1.pickle')

with open(nodes_file_location,'r') as f:
	vals=f.read()
minions=vals.split('\n')

print('Following nodes in the cluster',minions)

for m in minions:
	port,ip=m.split(':')
	ips.append(ip)
	ports.append(port)	
# print(nodes.keys())

	
if not os.path.isfile(file_table_location):
	with open(file_table_location,'wb') as ftl:
		pickle.dump(dict(),ftl)
	fl=open(file_table_location,'rb')
	file_table=pickle.load(fl)
else:
	fl=open(file_table_location,'rb')
	file_table=pickle.load(fl)
def check_file_table():
	if not os.path.isfile(file_table_location):
		print('File Table Does not exist yet')

def get_file_table_listing(f_t=file_table):	
	for p in f_t.keys():
		print('Files in the node %s:\n'%p)
		for fd_name in f_t[p].keys():
			print(fd_name+'\n')
			for blcks in list(f_t[p][fd_name]):
				print(blcks)
			print('\n')

def upload(file,row_value):
	data=pd.read_csv(input_file)
	# number_of_files_estimation=[]

	data_category_range=data[row_value].unique()
	data_category_range = data_category_range.tolist()
	folder_name=''.join(input_file.split('/')[-1].split('.')[:-1])
	folder_name=str(folder_name)
	for i,value in enumerate(data_category_range):
		content=pickle.dumps(data[data[row_value] == value])
		# number_of_files_estimation.append(content)
		file_name=value
		
	print('Sharding based on %s will generate %d files.\n'%(row_value,len(data_category_range)))
	port=int(random.choice(ports))
	ip=ips[0]
	c=rpyc.connect(ip,port=port)
	m=c.root.write_data(content,folder_name,file_name)	
	if port not in file_table.keys():
		file_table[port]={}
		if folder_name not in file_table[port].keys():

			file_table[port][folder_name]=[]
			file_table[port][folder_name].append(m)
		elif folder_name in file_table[port].keys():
			file_table[port][folder_name].append(m)
	elif port in file_table.keys():
		if folder_name not in file_table[port].keys():

			file_table[port][folder_name]=[]
			file_table[port][folder_name].append(m)
		elif folder_name in file_table[port].keys():
			file_table[port][folder_name].append(m)

	with open(file_table_location,'ab') as out:
		pickle.dump(file_table,out)
	get_file_table_listing(f_t=file_table)
while True:
	user_choice=input('What would you like to do?\n')
		# print('Available choice\n1.Upload File::Command::put')
	if user_choice.lower()=='put':
		k=input('Enter file location\n')
		j=input('Enter the value based on which you would like to fragment the file\n')
		
		confirm_sharding=input('Would you like to confirm? (Y|N)\n ')
		if confirm_sharding.lower()=='y':
			upload(k,j)