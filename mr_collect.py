__author__ = 'pengzhan'

import os
import sys
if __name__ == '__main__':
    #print os.path.realpath(__file__)
    filename_base = sys.argv[1]
    output_filename = sys.argv[2]

    map = {}
    if not os.path.exists(os.getcwd()+'/'+filename_base):
        print 'directory don\'t exist'
    else:
        for file in os.listdir(os.getcwd()+'/'+filename_base):
            print file
            if file.startswith(filename_base) and file != (output_filename+'.txt'):
                rf = open(os.getcwd()+'/'+filename_base+'/'+file,'r')
                for line in rf:
                    pair = line.split(':')
                    if map.has_key(pair[0]):
                        map[pair[0]] = map[pair[0]]+int(pair[1])
                    else:
                        map[pair[0]] = int(pair[1])
        f = open(os.getcwd()+'/'+filename_base+'/'+output_filename+'.txt','w')
        #print map
        for key in sorted(map):
            f.write(key+':'+str(map[key])+'\n')
        f.close()

