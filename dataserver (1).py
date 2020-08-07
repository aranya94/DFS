import rpyc
from rpyc import ThreadedServer
import os
import uuid
import sys
import subprocess
import threading
from queue import Queue


q=Queue()
lock=threading.Lock()


# prid=os.getpid()
data_location=os.path.expanduser('~')
data_location+='/Desktop/%s/'%sys.argv[1]
k=''

port=int(sys.argv[1])
# print(prid)

class DataService(rpyc.Service):
	global prid
	def exposed_write_data(self,data,i_file):
		global data_location
		f_name=uuid.uuid1()
		folder=data_location+i_file+'/'
		if not os.path.isdir(folder):
			
			os.mkdir(folder)
		exposed_filename=folder+'%s.txt'%f_name
		f=open(exposed_filename,'w')
		if data!='':
			for c in range(0,len(data)):
				f.write(data[c])
		f.close()
		return str(f_name)
	def exposed_fetch_data(self,block_id,folder_name):
		global data_location
		folder=data_location+folder_name+'/'
		fetch_name=folder+'%s.txt'%block_id
		j=open(fetch_name,'r')
		content=j.read()
		open_files=subprocess.getoutput("lsof -a -p %s +D /home/aranya/Desktop/ -F n  |sort"%prid)
		return open_files,content

	# def exposed_open_files(self):
	# 	open_files=subprocess.getoutput("lsof -a -p %s +d /home/aranya/Desktop/ -F n  |sort|uniq"%prid)
	# 	return open_files
	





	
	# def th_start(file_list):
	# 	for i in range(len(file_list)):
	# 		t=threading.Thread(target=worker)
	# 		t.daemon=True
	# 		t.start()
	# 	for file in file_list:
	# 		q.put(file)
	# 	q.join()

	def exposed_submit_mr_job(self,content,location):
		global k
		fn='mrjobfile%s'%uuid.uuid1()
		with open(os.path.expanduser('~')+'/Desktop/%s.py'%fn,'w') as mrjobfile:
			mrjobfile.write(content)
		mr_file_location=os.path.expanduser('~')+'/Desktop/'+fn+'.py'
		sub_folder=data_location+location
		file_list=list(os.listdir(sub_folder))
		
		for file in file_list:
			
			
		# # th_start(file_list)
			file_val=sub_folder+'/'+file
			k+=subprocess.getoutput('python %s %s'%(mr_file_location,file_val))
			print(k)
			return k
		# 	k+=str(os.system('python %s %s'%(mr_file_location,file_val)))
		# print(k)

		# os.system('python %s %s'%(mr_file_location,location))

	



if __name__=='__main__':
	if not os.path.isdir(data_location):
		os.mkdir(data_location)
	t=ThreadedServer(DataService,port=port)
	t.start()


