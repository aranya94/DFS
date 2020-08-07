import os
import sys
d=dict()

# file_list=list(os.listdir(os.path.expanduser('~')+'/Desktop/%s'%sys.argv[1]))
# for file in file_list:
text=open(sys.argv[1],'r')
for line in text:
	line=line.strip()
	line=line.lower()
	words=line.split(' ')
	for word in words:
		word=word.replace('.','').replace(',','').replace('!','').replace('@','').replace(';','').replace('[','').replace(']','').replace('(','').replace(')','').replace('-','').replace('?','')
		if word in d:
			d[word]=d[word]+1
		else:
			d[word]=1
for key in list(d.keys()):
	print(key,":",d[key])
	

