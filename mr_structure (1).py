import os
import pickle
import rpyc




fl=open('/home/aranya/Desktop/filetable.pickle','rb')
filetable=pickle.load(fl)
print(filetable.keys())
ips=list(filetable.keys())
# print(filetable.items())
mr_job_file=input('ENter the location of the MR file\n')
with open(mr_job_file,'r') as file1:
	content=file1.read()
# print(content)
file_for_mr=input('Enter the name of the file on which you want to run word count\n')
fmr=file_for_mr.split('/')[-1].split('.')[0]
for ip in ips:
	# print (filetable[ip].keys())
	if fmr in filetable[ip].keys():
		 c=rpyc.connect('localhost',port=ip)
		 location=str(ip)+'/'+fmr
		 print('File is present in:\n'+str(ip)+location)
		 c.root.submit_mr_job(content,location)
