
import argparse
from sortedcollections import OrderedSet
import random
import math
import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
import statistics


def dist(a, b):
    sum = 0.0
    for i in range(len(a)):
        sum += (a[i] - b[i]) * (a[i] - b[i])
    return math.sqrt(sum)

def proc():
    # Data format: point_idx,distance from its nearest centroid.
    # Point clusters are separated by a newline
    filename = "point_distance_data_clusters.txt"
    #filename = "smalltest.txt"
    f = open(filename, 'r')
    contents = f.readlines()
    f.close()
    # format list properly into 50 clusters of datapoints, each w format: (int, float)
    current_cluster_idx = 0
    clusters = []
    # initialize 50 empty clusters
    for i in range(0,50):
        clusters.append([])
    first_in_cluster = True
    for j in range(0, len(contents)):
        if contents[j] == '\n':
            if first_in_cluster:
                current_cluster_idx += 1
                first_in_cluster = False
            continue
        # set back to first in cluster once next point is here
        first_in_cluster = True
        this_point = contents[j].strip().split(',')
        idx = int(this_point[0])
        distance = float(this_point[1])
        clusters[current_cluster_idx].append((idx, distance))
    #print(clusters)

    #list with 50 entries, for each cluster's mean and SD
    cluster_stats = []
    filename = "clusters_mean_sd.txt"
    wf = open(filename, 'w')
    for cluster in clusters:
        pure_data = [x[1] for x in cluster]
        mean = sum(pure_data) / float(len(pure_data))
        sd = statistics.pstdev(pure_data)
        cluster_stats.append((mean, sd))
        # Write to file
        wf.write(str(mean) + ',' + str(sd) + '\n')



def main():
    proc()

if __name__== "__main__":
    main()


























