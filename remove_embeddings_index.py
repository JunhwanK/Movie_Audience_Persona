
import argparse
from sortedcollections import OrderedSet
import random
import math
import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
import csv


def proc():
    filename = "train_embeddings.csv"
    f = open(filename, 'r')
    contents = f.readlines()
    f.close()
    print(len(contents))
    roundNum = 1
    new_centroids_filename = "pure_embeddings.csv"
    cf = open(new_centroids_filename, 'w')
    for line in contents:
        split_x = line.split(',')
        float_x = [str(elt) for elt in split_x]
        y = float_x[1:]
        newline = ','.join(y)
        #print(newline)
        cf.write(newline)



def main():
    proc()

if __name__== "__main__":
    main()


























