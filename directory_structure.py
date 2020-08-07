import os
import pprint

pp = pprint.PrettyPrinter()

def path_to_dict(path, d):

    name = os.path.basename(path)

    if os.path.isdir(path):
        if name not in d['dirs']:
            d['dirs'][name] = {'dirs':{},'files':[]}
        for x in os.listdir(path):
            path_to_dict(os.path.join(path,x), d['dirs'][name])
    else:
        d['files'].append(name)
    return d


mydict = path_to_dict('/home/aranya/Desktop/18812', d = {'dirs':{},'files':[]})

# pp.pprint(mydict)
print(mydict['dirs']['dirs'])