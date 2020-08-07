import rpyc
import sys
import os
import math
import pickle
chunksize=100000
open_files_by_program=[]
home=os.path.expanduser('~')
score_table={}
total_score=0
scores_file_location=os.path.expanduser('~')+'/Desktop/score_check.txt'
file_table={}
ips=[]
files_count={}
with open(scores_file_location,'r') as f:
	vals=f.read()
minions=vals.split('\n')
minions.pop()
for m in minions:
	score,port,ip=m.split(':')	
	score_table[score]=port
	ips.append(ip)
for h in score_table.keys():
	total_score+=int(h)
	total_score=int(total_score)

def get_file_table_listing(f_t=file_table):
	
	for p in f_t.keys():
		print('Files in the node %s:\n'%p)
		for fd_name in f_t[p].keys():
			print(fd_name+'\n')
			for blcks in list(f_t[p][fd_name]):
				print(blcks)
			print('\n')

def block_placement(i_file):
	global total_score
	global score_table
	# global  input_file
	#global file_table

	number_of_chunks=int(math.ceil((os.stat(i_file).st_size/chunksize+1)))
	print('Total number of chunks for the file is:%d '%number_of_chunks)
	input_file=open(i_file,'r')
	scores_of_nodes=[int(q)/total_score for q in score_table.keys()]
	ports=[int(p) for p in score_table.values()]
	# print(scores_of_nodes)
	for i in scores_of_nodes:
		print('The scores of the nodes are as follows:%f'%i)

	n=0

	while n<len(ports):
		chunks_per_node=int(scores_of_nodes[n]*number_of_chunks)
		port=ports[n]
		ip=ips[n]
		folder_name=i_file.split('/')[-1].split('.')[0]
		folder_name=str(folder_name)
		for r in range(0,chunks_per_node):
		 	c=rpyc.connect(ip,port=port)
		 	data=input_file.readlines(chunksize)

		 	m=c.root.write_data(data,folder_name)
		 
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

		print('Sent %d chunks to:'%chunks_per_node+'%d'%port)
		n+=1
		# print('List of files in node : %d \n'%port)
		# for f_n in list(file_table[port].keys()):
		# 	print(f_n)
		# 	print('List of blocks for the file %s:\n'%f_n)
		# 	for list_of_blocks_per_file in list(file_table[port][folder_name]):

		# 		print(list_of_blocks_per_file)
		# print('\n')
	with open('/home/aranya/Desktop/filetable.pickle','wb') as out:
		pickle.dump(file_table,out)
	get_file_table_listing(f_t=file_table)

def fetch_block_data():
	global file_table
	result=''
			
	
	block_id=input('Enter the block_id:\n')
	folder_name=input('Enter the folder_name:\n')
	for prts in file_table.keys():
		if folder_name in file_table[prts]:

			if block_id in file_table[prts][folder_name]:
				select_port=int(prts)


	c=rpyc.connect('localhost',port=select_port)
	o_files,result=c.root.fetch_data(block_id,folder_name)
	# result+=c.root.fetch_data(block_id,folder_name)
	# open_files_by_program.append(c.root.open_files())
	open_files_by_program.append(o_files)		
	print('*'*20+'Beginning of the file'+'*'*20+'\n')
	print(result)
	print('*'*20+'End of the file'+'*'*20+'\n')
	print(open_files_by_program)
	# for o_f in open_files_by_program:
	# 	if o_f in files_count:
	# 		files_count[o_f]=files_count[o_f]+1
	# 	else:
	# 		files_count[o_f]=1
	# print(files_count)
# block_placement()
print('*'*30+'Welcome'+'*'*30)
def clear_screen():
	if os.name=='nt':
		_=os.system('cls')
	else:
		_=os.system('clear')
while (1):

	user_choice=input('What operation do you want to perform:\n')
	if user_choice=='put':
		file_to_be_put=input('Enter the location of the file:\n')
		block_placement(i_file=file_to_be_put)
	elif user_choice=='get':
		get_file_table_listing()
		fetch_block_data()
	elif user_choice=='exit':
		sys.exit()
	elif user_choice=='clear':
		clear_screen()
	elif user_choice=='open':
		print(open_files_by_program)
	else:
		print('Check correct usage\nAvailable options:\n1.put\n2.get')
