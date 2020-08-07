import pandas as pd 
import math
#print(pd.read_csv('/home/aranya/Desktop/DATASETS/hospital_directory.csv', nrows=1).columns)
data=pd.read_csv('/home/aranya/Desktop/DATASETS/hospital_directory.csv')
data_category_range=data['State'].unique()
data_category_range = data_category_range.tolist()
print(data_category_range)
chunks=[]
total_files=len(data_category_range)
ratio=[2/5,3/5]
while n<len(ratio):
	c_p_n=ratio[n]*

n=0

print(total_files)

while n<len(ratio):
	
	
	for k in range(0,ratio[n]):
		#for i,value in enumerate(data_category_range):
		content=data[data['State'] == data_category_range[k]]
		
		print(data_category_range[k])
	data_category_range=data_category_range[k+1:]	
	print("New Node-------------------------------------------")
	
	n+=1
	  
#     content.to_csv(r'/home/aranya/Desktop/check/'+str(value.replace('/',''))+r'.csv',index = False, na_rep = 'N/A')
