import pickle
k=''
test_list=[1,2,3]
print(test_list)
with open('/home/aranya/Desktop/check.pickle','wb') as file1:#saving to object
	pickle.dump(test_list,file1)#object to dump,where to dump
with open('/home/aranya/Desktop/check.pickle','rb') as file2:
	content=pickle.load(file2)#loading object from pickle file
print(content)
content[0]#content is still of object type list so can use list methods on content like list slice
print(type(content))
for l in content:
	k+=str(l)+'\n'
print(k)