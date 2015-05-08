__author__ = 'blu2'
import os


def split_file(filename, num_worker):
    fileSize = os.path.getsize(filename)
    chunks = []
    split_size = fileSize/num_worker
    with open(filename) as inputfile:
        current_size = 0
        offset = 0
        #outputfile = open('sub_inputfile_' + str(index) + '.txt', 'w')
        for line in inputfile:
            #print line
            current_size += len(line)
            offset += len(line)
            if current_size >= split_size:
                current_size -= len(line)
                offset -= len(line)
                words = line.split(' ')
                #print words
                for w in words:
                    current_size += len(w)
                    offset += len(w)
                    if not w.endswith('\n'):
                        current_size += 1
                        offset += 1
                    print current_size
                    if current_size >= split_size:
                        done = True
                        chunks.append((current_size, offset-current_size))
                        current_size = 0


        #the last chunk
        if current_size > 0:
            chunks.append((current_size, offset-current_size))

    # very special case
    if len(chunks) < num_worker:
        chunks.append((current_size, offset))

    return chunks

print split_file("inputfile2.txt", 4)

