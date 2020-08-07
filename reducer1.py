import os
import sys
with open(sys.argv[1], "r") as file1:
    text=file1.read() 
  
# Create an empty dictionary 
d = dict() 
  
lines=text.split('\n')
for line in lines:
    line=line.strip()
    word,count=line.split(' : ',1)
    if word in d.keys():
        d[word]=d[word]+count
    else:
        d[word]=count

for key in list(d.keys()): 
    print(key, ":", d[key])
with open(os.path.expanduser('~'+'/Desktop/finaloutput2.txt'),'w') as file1:
    file1.write(str(d))
print(k)