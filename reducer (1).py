import os
import sys

d={}
k=''
with open(sys.argv[1],'r') as file1:
	content=file1.read()
lines=content.split('\n')
for line in lines:
	word,count=line.split(' : ',1)
	if word not in d.keys():
		d[word]=count
	else:
		d[word]=d[word]+count
for key in list(d.keys()):
	k+=key+':'+str(d[key])+'\n'
with open(os.path.expanduser('~')+'/Desktop/finaloutput.txt','w') as file2:
	file2.write(str(d))
print(k)

# os.remove(sys.argv[1])