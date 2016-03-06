#!/usr/bin/python

import sys
import os

if sys.argv[1] == '--help' or sys.argv[1] == '-h':
    print 'usage: python merge_partitions.py bmatrix_file_prefix'
else:
    prefix = sys.argv[1]
    files_to_merge = []
    print '# Looking for files with prefix: ', prefix
    for file in os.listdir("./"):
        if file.startswith(prefix):
            files_to_merge.append(file)
    print '# files to be merged: ', files_to_merge

    merged_matrix = {}
    for file in files_to_merge:
        f = open(file, 'r')
        for line in f:
            if not line.startswith("#"):
                split_line = line.split()
                key0 = split_line[0]
                key1 = split_line[1]
                val = int(split_line[2])
                if key0 in merged_matrix:
                    if key1 in merged_matrix[key0]:
                        merged_matrix[key0][key1] += val
                    else:
                        merged_matrix[key0][key1] = val
                else:
                    merged_matrix[key0] = {key1: val}
        f.close()

    for key0, matrix2 in merged_matrix.iteritems():
        for key1, value in matrix2.iteritems():
            print "{0}\t{1}\t{2}".format(key0, key1, value)
