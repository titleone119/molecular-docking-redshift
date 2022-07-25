#! /usr/bin/env python

import sys
import glob

def doit(n):
    file_names = glob.glob('*/*.pdbqt')
    everything = []
    failures = []
    print 'Found', len(file_names), 'pdbqt files'
    for file_name in file_names:
        file = open(file_name)
        lines = file.readlines()
        file.close()
        try:
            line = lines[1]
            result = float(line.split(':')[1].split()[0])
            everything.append([result, file_name])
        except:
            failures.append(file_name)
    everything.sort(lambda x,y: cmp(x[0], y[0]))
    part = everything[:n]
    for p in part:
        print p[1],
    print
    if len(failures) > 0:
        print 'WARNING:', len(failures), 'pdbqt files could not be processed'

if __name__ == '__main__':
    doit(int(sys.argv[1]))