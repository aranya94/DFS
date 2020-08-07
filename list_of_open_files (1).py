from functools import reduce

import pickle
query_and_associated_files=pickle.load(open('/home/aranya/Desktop/opened_files.pickle','rb'))

# query_and_associated_files={'q1':[['b1','b2','b3','b7'],['b4','b5','b1']],'q2':[['b2','b3','b4'],['b9','b4','b7','b8'],['b11','b5','b4'],['b4']]}
files_count={}
most_used_files=[]
# if query_name not in query_and_associated_files.keys():
# 	query_and_associated_files[query_name]=[]
# 	query_and_associated_files[query_name].append(list_of_open_files)
# else:
# 	query_and_associated_files[query_name]+list_of_open_files
for f in query_and_associated_files:
	query_and_associated_files[f]=reduce(lambda x,y:x+y,query_and_associated_files[f])


list_of_open_files=list(query_and_associated_files.values())


for files_list in list_of_open_files:
	for opened_file in files_list:
		if opened_file in files_count.keys():
			files_count[opened_file]=files_count[opened_file]+1
		else:
			files_count[opened_file]=1
def total_count():
	count=0
	for f in files_count:
		k=files_count[f]
		count+=k
	return count
for f in files_count:
	print(f+':'+str(files_count[f]/total_count()))
	if files_count[f]/total_count()>=0.5:
		most_used_files.append(f)
print(total_count())
print(files_count)
print(most_used_files)
