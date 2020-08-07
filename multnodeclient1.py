import rpyc
import os
import pickle
import pandas as pd
import random
import sys
import matplotlib.pyplot as plt
from functools import reduce



ips=[]
ports=[]

# nodes={}
count_of_mr=0
cluster_nodes={}

nodes_file_location=os.path.expanduser('~')+'/Desktop/nodes_file.txt'
file_table_location=os.path.expanduser('~/Desktop/file_table_store.pickle')
mr_service_location=os.path.expanduser('~')+'/Desktop/PROTOTYPE/MR_STRUCTURE2.py'
rebalancer_service_location=os.path.expanduser('~')+'/Desktop/project/list_of_open_files.py'
frquency_file_location=os.path.expanduser('~')+'/Desktop/frequency_table1.pickle'
sub_index_file_location=os.path.expanduser('~')+'/Desktop/sub_index.pickle'
if not os.path.isfile(sub_index_file_location):
	with open(sub_index_file_location,'wb') as sifl:
		pickle.dump(dict(),sifl)
	gh=open(sub_index_file_location,'rb')
	sub_index=pickle.load(gh)
else:
	gh=open(sub_index_file_location,'rb')
	sub_index=pickle.load(gh)

with open(nodes_file_location,'r') as f:
	vals=f.read()
vals=vals.strip()
minions=vals.split('\n')

print('Following nodes in the cluster',minions)

for m in minions:
	port,ip=m.split(':')
	# ips.append(ip)
	# ports.append(port)	
# print(nodes.keys())
	# cluster_nodes[ip]=int(port)  ######undo this if does not work
	cluster_nodes[port]=ip
	
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
			for sub_folder in f_t[p][fd_name].keys():
				print(sub_folder+'\n')
				for blcks in list(f_t[p][fd_name][sub_folder]):
					print(blcks)
			print('\n')

def read_data(input_file,row_value):

	data=pd.read_csv(input_file)
	# number_of_files_estimation=[]
	
	data_category_range=data[row_value].unique()
	data_category_range = data_category_range.tolist()
	print('Sharding based on %s will generate %d files.\n'%(row_value,len(data_category_range)))
	siv=[]
	while True:
		index_val=input('Enter the value on which you would like to sub index the file for further searches\n.E to stop!!')
		if index_val.upper()=='E':
			break
		else:
			siv.append(index_val)
	confirm_sharding=input('Would you like to confirm? (Y|N)\n ')
	if confirm_sharding.lower()=='y':

		folder_name=''.join(input_file.split('/')[-1].split('.')[:-1])
		folder_name=str(folder_name)
		for i,value in enumerate(data_category_range):
			file_content=data[data[row_value] == value]
			content=pickle.dumps(file_content)
			for ax in siv:
				if folder_name  in sub_index.keys():
					if value in sub_index[folder_name].keys():

						sub_index[folder_name][value]+list(file_content[ax].unique())
					else:
						sub_index[folder_name][value]=list(file_content[ax].unique())
				else:
					sub_index[folder_name]={}
					if value in sub_index[folder_name].keys():

						sub_index[folder_name][value]+list(file_content[ax].unique())
					else:
						sub_index[folder_name][value]=list(file_content[ax].unique())
					
		# number_of_files_estimation.append(content)
			file_name=str(value)
			# ip=ips[0]
			# port=int(random.choice(ports))
			port=random.choice(list(cluster_nodes.keys()))
			ip=cluster_nodes[port]
			loc=str(ip+':'+str(port))
			c=rpyc.connect(ip,port=int(port))
			m=c.root.write_data(content,folder_name,file_name,row_value)
		# infiles.append([content,folder_name,file_name,ip,port])
			if loc not in file_table.keys():
				file_table[loc]={}
				if folder_name not in file_table[loc].keys():

					file_table[loc][folder_name]={}
					if row_value not in file_table[loc][folder_name].keys():
						file_table[loc][folder_name][row_value]=[]
						file_table[loc][folder_name][row_value].append(file_name)
					elif row_value in file_table[loc][folder_name].keys():
						file_table[loc][folder_name][row_value].append(file_name)

				elif folder_name in file_table[loc].keys():
					if row_value not in file_table[loc][folder_name].keys():
						file_table[loc][folder_name][row_value]=[]
						file_table[loc][folder_name][row_value].append(file_name)
					elif row_value in file_table[loc][folder_name].keys():
						file_table[loc][folder_name][row_value].append(file_name)
			elif loc in file_table.keys():
				if folder_name not in file_table[loc].keys():
					file_table[loc][folder_name]={}
					if row_value not in file_table[loc][folder_name].keys():
						file_table[loc][folder_name][row_value]=[]
						file_table[loc][folder_name][row_value].append(file_name)
					elif row_value in file_table[loc][folder_name].keys():
						file_table[loc][folder_name][row_value].append(file_name)
				elif folder_name in file_table[loc].keys():
					
					if row_value not in file_table[loc][folder_name].keys():
						file_table[loc][folder_name][row_value]=[]
						file_table[loc][folder_name][row_value].append(file_name)
					elif row_value in file_table[loc][folder_name].keys():
						file_table[loc][folder_name][row_value].append(file_name)

		with open(file_table_location,'wb') as out:
			pickle.dump(file_table,out)
		with open(sub_index_file_location,'wb') as out1:
			pickle.dump(sub_index,out1)
	else:
		return "okay"
		

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
			if files_count[file]/len(all_files)>0.01:
				most_used_files.append(file)
		# print("These are the most used files:\n",most_used_files)
		plt.bar(range(len(qq)),list(qq.values()),align='center')
		plt.xticks(range(len(qq)),list(qq.keys()))
		plt.show(block=True)
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

def del_folder(folder_name,sub_folder):
	for loc,folders in file_table.items():
		if folder_name in file_table[loc].keys():
			if sub_folder in file_table[loc][folder_name].keys():
				del file_table[loc][folder_name][sub_folder]				
				ip,port=loc.split(':')
				port=int(port)
				c=rpyc.connect(ip,port=port)
				c.root.delete_files(folder_name+'/'+sub_folder)
	with open(file_table_location,'wb') as out:
			pickle.dump(file_table,out)



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
	# elif user_choice=='mr':
	# 	mr_service()
		# count_of_mr+=1
		# if count_of_mr==3:
		# 	rebalancer()
	elif user_choice=='bench':
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
	elif user_choice=='del':
		main_f_name=input('Enter the name of the original file')
		sub_f_name=input('Enter the name fo the sub folder you want to delete')
		del_folder(main_f_name,sub_f_name)

	else:
		print('Check correct usage\n1.Upload File::Command::put\n')







# main_interface()


