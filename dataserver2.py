import rpyc
from rpyc import ThreadedServer
import pickle
import csv
import os
import sys
import pandas as pd
import uuid
import subprocess
import glob
import shutil
data_location=os.path.expanduser('~')
data_location+='/Desktop/%s/'%sys.argv[1]
port=int(sys.argv[1])
# results=[]
class DataService(rpyc.Service):
	def exposed_write_data(self,data,folder_name,file_name,row_value):
		
		folder_name=data_location+folder_name+'/'
		if not os.path.isdir(folder_name):
			os.mkdir(folder_name)
		if not os.path.isdir(folder_name+row_value):
			os.mkdir(folder_name+row_value)
		f_name=folder_name+row_value+'/'+file_name+'.csv'
		content=pickle.loads(data)
		content.to_csv(f_name,index=False)
		return file_name

	def query_engine(self,file_val,command):
		file_name=file_val
		# print(file_name)
		df=pd.read_csv(file_name)
		mask=eval(command)
		res=mask
		# output_file=os.path.expanduser('~')+'/Desktop/res%s.csv'%uuid.uuid1()
		# res.to_csv(output_file,index=False,na_rep='N/A')
		return res
	def exposed_perform_query(self,query,folder_name,chosen_sub,file_name):
		results=[]
		global k
		command=query
		files_used=[]
		# print('Has reached the data servers\n\n')
		sub_folder=data_location+folder_name+'/'+chosen_sub+'/'
		
		# file_list=list(os.listdir(sub_folder))
		
		# for file in file_list:
		# 	if str(file)==file_name+'.csv':
		file_val=str(sub_folder+file_name)
		# print('Reading file : %s \n'%file_val)
		k=self.query_engine(file_val,command)
		results.append(k)
		files_used.append(file_val)
		
		# df_from_each_file=(pd.read_csv(f) for f in results)
		# concatenated_df=pd.concat(df_from_each_file).drop_duplicates().reset_index(drop=True)
		concatenated_df=pd.concat(results).drop_duplicates()
		pkl_fl=pickle.dumps(concatenated_df)
		# os.remove(query_file_location)
		# for f in results:
		# 	os.remove(f)
		return pkl_fl,concatenated_df,files_used
	
	def exposed_perform_query2(self,query,folder_name):
		m={}
		results=[]
		global j
		command=query
		sub_folder=data_location+folder_name+'/'
		
		# print('Has reached the dataservers:\n\n\n')
		files_list=list(os.listdir(sub_folder))
		for file in files_list:
			
			file_val=str(sub_folder+file)
			
			j=self.query_engine(file_val,command)
			results.append(j)
			# df_from_each_file=(pd.read_csv(f) for f in results)
		concatenated_df=pd.concat(results).drop_duplicates()
		pkl_fl=pickle.dumps(concatenated_df)
			# os.remove(query_file_location)
			# for f in results:
			# 	os.remove(f)
		return pkl_fl,concatenated_df,files_list
	def exposed_delete_files(self,folder_name):
		sub=data_location+folder_name+'/'
		shutil.rmtree(sub)
			

			





if __name__=='__main__':
	if not os.path.isdir(data_location):
		os.mkdir(data_location)
	t=ThreadedServer(DataService,port=port)
	t.start()