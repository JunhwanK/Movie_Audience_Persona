
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

def get_mean_sd():
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
    return clusters, cluster_stats

# Returns list of outlier point indices to remove
def zscore(clusters,mean_sd):
    count = 0
    points_to_remove = []
    for i in range(len(clusters)):
        one_cluster = clusters[i]
        cluster_mean = mean_sd[i][0]
        cluster_sd = mean_sd[i][1]
        #print(cluster_mean)
        #print(cluster_sd)
        for one_point in one_cluster:
            idx = int(one_point[0])
            distance = one_point[1]
            zscore = (distance - cluster_mean) / cluster_sd
            if zscore >= 3.0:
                print(str(idx) + ',' + str(zscore))
                points_to_remove.append(idx)
                count += 1
    print(count)
    return points_to_remove

def trim_train_embeddings(outliers):
    filename = "train_embeddings.csv"
    f = open(filename, 'r')
    contents = f.readlines()
    f.close()
    trimfile = "trim_embeddings.csv"
    tf = open(trimfile, 'w')
    print(len(contents)) # check before and after
    count = 0
    for line in contents:
        split_x = line.split(',')
        idx = int(split_x[0])
        if idx in outliers:
            continue # do not write to trimfile if it's an outlier
        convert_x = [str(elt) for elt in split_x]
        y = convert_x[1:]
        newline = ','.join(y)
        tf.write(newline)
        count += 1
    print(count)


def main():
    clusters, mean_sd = get_mean_sd()
    outliers = zscore(clusters,mean_sd)
    trim_train_embeddings(outliers)


if __name__== "__main__":
    main()


























