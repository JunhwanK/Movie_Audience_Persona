
import argparse
from sortedcollections import OrderedSet
import random
import math
import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt


def proc():
    filename = "train_embeddings.csv"
    f = open(filename, 'r')
    contents = f.readlines()
    f.close()
    print(len(contents))
    roundNum = 1
    centroids_filename = "embeddings_centroids.csv"
    cf = open(centroids_filename, 'w')
    centroids = []
    for i in range(0, len(contents), 2950):
         cf.write(contents[i])
         centroids.append(contents[i])
    print(centroids)
    print(len(centroids))
    return(centroids)


def main():
    proc()

if __name__== "__main__":
    main()


























