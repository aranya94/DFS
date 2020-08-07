import os
import sys
import uuid

chunksize=100000
input_file=open(sys.argv[1],'r')
home=os.path.expanduser('~')
storage_dir=home+'/Desktop/PFS/'

for i in range(0,int(os.stat(sys.argv[1]).st_size/chunksize)+1):
    f = open(storage_dir+'file%s'%uuid.uuid1()+'.txt','w')
    segment = input_file.readlines(chunksize)
    if segment!='':
    	for c in range(0,len(segment)):
        	f.write(segment[c]+"\n")
    f.close()
