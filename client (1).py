import rpyc
import sys
import os
import math
chunksize=100000
input_file=open(sys.argv[1],'r')
home=os.path.expanduser('~')
score_table={}
total_score=0
scores_file_location=os.path.expanduser('~')+'/Desktop/score_check.txt'
file_table={}
with open(scores_file_location,'r') as f:
	vals=f.read()
minions=vals.split('\n')
minions.pop()
for m in minions:
	score,port=m.split(':')	
	score_table[score]=port

for h in score_table.keys():
	total_score+=int(h)
	total_score=int(total_score)

	

number_of_chunks=int(math.ceil((os.stat(sys.argv[1]).st_size/chunksize+1)))
print('Total number of chunks for the file is:%d '%number_of_chunks)


scores_of_nodes=[int(q)/total_score for q in score_table.keys()]
ports=[int(p) for p in score_table.values()]
print(scores_of_nodes)
for i in scores_of_nodes:
	print('The scores of the nodes are as follows:%f'%i)

n=0

while n<len(ports):
	chunks_per_node=int(scores_of_nodes[n]*number_of_chunks)
	port=ports[n]
	
	for r in range(0,chunks_per_node):
	 	c=rpyc.connect('localhost',port=port)
	 	data=input_file.readlines(chunksize)

	 	m=c.root.write_data(data)
	 
	 	if port not in file_table:
	 		file_table[port]=[]
	 		file_table[port].append(m)
	 	else:
	 		file_table[port].append(m)
	print('Sent %d chunks to:'%chunks_per_node+'%d'%port)
	n+=1
	print(file_table[port])


result=''
	
block_id=input('Enter the block id you want to fetch')
for prts in file_table.keys():
	if block_id in file_table[prts]:
		select_port=int(prts)


c=rpyc.connect('localhost',port=select_port)
result+=c.root.fetch_data(block_id)
	

print(result)