with open('/home/aranya/Desktop/data/complaint-30.txt') as infile, open('/home/aranya/Desktop/outfile1.csv','w') as outfile: 
    for line in infile: 
        outfile.write(line.replace(' ',','))