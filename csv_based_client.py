import rpyc
import os
import pickle
import pandas as pd
import random
import sys
from functools import reduce



ips=[]
ports=[]

# nodes={}
count_of_mr=0

nodes_file_location=os.path.expanduser('~')+'/Desktop/nodes_file.txt'
file_table_location=os.path.expanduser('~/Desktop/file_table_store.pickle')
mr_service_location=os.path.expanduser('~')+'/Desktop/PROTOTYPE/MR_STRUCTURE2.py'
rebalancer_service_location=os.path.expanduser('~')+'/Desktop/project/list_of_open_files.py'
frquency_file_location=os.path.expanduser('~')+'/Desktop/frequency_table.pickle'

with open(nodes_file_location,'r') as f:
	vals=f.read()
vals=vals.strip()
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

def get_file_table_listing():
	fl=open(file_table_location,'rb')
	f_t=pickle.load(fl)


	for p in f_t.keys():
		print('Files in the node %s:\n'%p)
		for fd_name in f_t[p].keys():
			print(fd_name+'\n')
			for blcks in list(f_t[p][fd_name]):
				print(blcks)
			print('\n')

def read_data(input_file,row_value):

	data=pd.read_csv(input_file)
	# number_of_files_estimation=[]

	data_category_range=data[row_value].unique()
	data_category_range = data_category_range.tolist()
	print('Sharding based on %s will generate %d files.\n'%(row_value,len(data_category_range)))
		
	confirm_sharding=input('Would you like to confirm? (Y|N)\n ')
	if confirm_sharding.lower()=='y':

		folder_name=''.join(input_file.split('/')[-1].split('.')[:-1])
		folder_name=str(folder_name)
		for i,value in enumerate(data_category_range):
			content=pickle.dumps(data[data[row_value] == value])
		# number_of_files_estimation.append(content)
			file_name=value
			ip=ips[0]
			port=int(random.choice(ports))
			c=rpyc.connect(ip,port=port)
			m=c.root.write_data(content,folder_name,file_name)
		# infiles.append([content,folder_name,file_name,ip,port])
			if port not in file_table.keys():
				file_table[port]={}
				if folder_name not in file_table[port].keys():

					file_table[port][folder_name]=[]
					file_table[port][folder_name].append(file_name)
				elif folder_name in file_table[port].keys():
					file_table[port][folder_name].append(file_name)
			elif port in file_table.keys():
				if folder_name not in file_table[port].keys():

					file_table[port][folder_name]=[]
					file_table[port][folder_name].append(file_name)
				elif folder_name in file_table[port].keys():
					file_table[port][folder_name].append(file_name)
		with open(file_table_location,'wb') as out:
			pickle.dump(file_table,out)
	else:
		return "okay"
		
# def upload_file(file):
# 	# global file_table
# 	# global file_table
# 	content=file[0]
# 	folder_name=file[1]
# 	file_name=file[2]
# 	ip=file[3]
# 	port=file[4]
# 	
		
	# if port not in file_table.keys():
	# 	file_table[port]={}
	# 	if folder_name not in file_table[port].keys():

	# 		file_table[port][folder_name]=[]
	# 		file_table[port][folder_name].append(file_name)
	# 	elif folder_name in file_table[port].keys():
	# 		file_table[port][folder_name].append(file_name)
	# elif port in file_table.keys():
	# 	if folder_name not in file_table[port].keys():

	# 		file_table[port][folder_name]=[]
	# 		file_table[port][folder_name].append(file_name)
	# 	elif folder_name in file_table[port].keys():
	# 		file_table[port][folder_name].append(file_name)
	# with open(file_table_location,'wb') as out:
	# 		pickle.dump(file_table,out)
	
	# get_file_table_listing()

# def main_interface():\
def clear_screen():
	if os.name=='nt':
		_=os.system('cls')
	else:
		_=os.system('clear')


def mr_service():
	os.system('python %s'%mr_service_location)


# def rebalancer():
# 	os.system('python %s'%rebalancer_service_location)
# def reset_open_files():
# 	opened_files_location='/home/aranya/Desktop/opened_files.pickle'
# 	with open(opened_files_location,'wb') as fxy:
# 		pickle.dump(dict(),fxy)

def frequent_check(lq,folder_name):
	
	if lq<=len(frequency_table[folder_name]):
		all_attributes=reduce(lambda x,y:x+y,frequency_table[folder_name])
		qq={}
		for i in all_attributes:
			if i not in qq.keys():
				qq[i]=1
			else:
				qq[i]+=1
		m=max(qq.values())
		highest_attribute=[]
		print('Count of attributes for the folder is as follows\n',qq)
		for j in qq.keys():
			if qq[j]==m:
				highest_attribute.append(j)
		# highest_attribute=max(set(all_attributes), key = all_attributes.count)
		second_set_attribute=reduce(lambda x,y:x+y,frequency_table[folder_name][-int(lq):])
		# print(second_set_attribute)
		second_highest_attribute=max(set(second_set_attribute), key = second_set_attribute.count)
		print('\n'*5)
		print('Most used attribute is\n',highest_attribute)
		print('Second most used attribute based on the last %s queries is'%lq)
		print(second_highest_attribute)
		all_files=reduce(lambda x,y:x+y,frequency_table['files'])
		files_count={}
		most_used_files=[]
		for f in  all_files:
			if f not in files_count.keys():
				files_count[f]=1
			else:
				files_count[f]=files_count[f]+1
		for file in files_count:
			if files_count[file]/len(all_files)>0.05:
				most_used_files.append(file)
		print("These are the most used files:\n",most_used_files)
	else:
		print("As of now there are %s number of history records"%len(frequency_table['attributes']))
	# print(all_files)
def recommender_system(filename):
	if not os.path.isfile(frquency_file_location):
		print( 'No records found as of yet from previous searches.The frequency table is built when you run more queries on the data\n\n')
	else:
		with open(frquency_file_location,'rb') as a:
			frequency_table=pickle.load(a)
		header_fields=list(pd.read_csv(k,nrows=1).columns)
		# print('Following header fields are available for the file\n',header_fields)
		header_fields=[i.upper() for i in header_fields]
		all_attributes=reduce(lambda x,y:x+y,frequency_table['attributes'])
		all_attributes=[i.upper() for i in all_attributes]
		
		
		
		recommended_attribute=set(header_fields).intersection(set(all_attributes))
		print('Based on previous queries this file can be fragmented based on:\n',recommended_attribute)



print('*'*10+'Welcome'+'*'*10)
while True:
	user_choice=input('What would you like to do?\n')
		# print('Available choice\n1.Upload File::Command::put')
	if user_choice.lower()=='put':
		k=input('Enter file location\n')
		
		
		recommender_system(k)
		header_fields=list(pd.read_csv(k,nrows=1).columns)
		print('Following header fields are available for the file\n',header_fields)
		j=input('Enter the value based on which you would like to fragment the file\n')
		
		read_data(k,j)
		
			
			# pool=Pool(len(ips))
			# pool.map(upload_file,infiles)
			

			
		get_file_table_listing()
		# else:
		# 	break
		
		# print('NOw here1')
		
		# print('Now here2')
		# get_file_table_listing()
	elif user_choice.lower()=='exit':
		sys.exit()
	elif user_choice=='get':
		check_file_table()

		get_file_table_listing()
	elif user_choice=='clear':
		clear_screen()
	elif user_choice=='mr':
		mr_service()
		# count_of_mr+=1
		# if count_of_mr==3:
		# 	rebalancer()
	elif user_choice=='sys':
		os.system('python /home/aranya/Desktop/PROTOTYPE/synthetic_benchmark_tool.py')
	elif user_choice=='eval':
		lq=int(input('How many queries do you want to check for second replica: \n'))
		fn=input('Enter the folder name on which you want to run evaluation:\n')
		if not os.path.isfile(frquency_file_location):
			print("The record file has been deleted/corrupted.Exiting\n")
		else:
			with open(frquency_file_location,'rb') as a:
				frequency_table=pickle.load(a)
		frequent_check(lq,fn)


	else:
		print('Check correct usage\n1.Upload File::Command::put\n')







# main_interface()


