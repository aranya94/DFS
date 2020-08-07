import os
import pickle
import threading
import rpyc
import sys
import subprocess
fl=open('/home/aranya/Desktop/file_table1.pickle','rb')
filetable=pickle.load(fl)
ports=list(filetable.keys())
map_file=input('Enter the location of the map file\n')
if not os.path.isfile(map_file):
	print('No such file exists\n')
	sys.exit()
with open(map_file,'r') as f1:
	content=f1.read()
reduce_file=input('Enter the location of the reduce file\n')
if not os.path.isfile(reduce_file):
	print('No such file exists\n')
	sys.exit()
target_file=input('Enter the name of the file on which you want to run Map Reduce\n')
if not os.path.isfile(target_file):
	print('No such file exists\n')
	sys.exit()
target_folder=''.join(target_file.split('/')[-1].split('.')[:-1])
print(target_folder)

for port,folders in filetable.items():
	if target_folder in filetable[port].keys():
		# location=str(port)+'/'+target_folder+'/'
		c=rpyc.connect('localhost',port=port)
		print('File is present in:\n'+str(port))
		interm_output=str(c.root.submit_mr_job(content,target_folder))
int_med_file_loc=os.path.expanduser('~')+'/Desktop/interm_output.txt'
with open(int_med_file_loc,'w') as inter_med:
	inter_med.write(interm_output)
print(subprocess.getoutput('python %s %s'%(reduce_file,int_med_file_loc)))
	