import rpyc
from rpyc import ThreadedServer
import pickle
import csv
import os
import sys
import pandas as pd




data_location=os.path.expanduser('~')
data_location+='/Desktop/%s/'%sys.argv[1]
port=int(sys.argv[1])
k=''



class DataService(rpyc.Service):
	def exposed_write_data(self,data,folder_name,file_name):
		
		folder_name=data_location+folder_name+'/'
		if not os.path.isdir(folder_name):
			os.mkdir(folder_name)
		f_name=folder_name+file_name+'.csv'
		content=pickle.loads(data)
		content.to_csv(f_name,index=False,na_rep='N/A')
		return file_name


	def exposed_submit_mr_job(self,content,location,sub_folders=sub_folders):

		fn='mrjobfile%s'%uuid.uuid1()
		with open(os.path.expanduser('~')+'/Desktop/%s.py'%fn,'w') as mrjobfile:
			mrjobfile.write(content)
		mr_file_location=os.path.expanduser('~')+'/Desktop/'+fn+'.py'
		sub_folder=data_location+location
		file_list=list(os.listdir(sub_folder))
		
		for file in file_list:
			
			
		# # th_start(file_list)
			file_val=sub_folder+'/'+file
	
			k+=str(subprocess.getoutput('python %s %s'%(mr_file_location,file_val)))
		print(k)


if __name__=='__main__':
	if not os.path.isdir(data_location):
		os.mkdir(data_location)
	t=ThreadedServer(DataService,port=port)
	t.start()