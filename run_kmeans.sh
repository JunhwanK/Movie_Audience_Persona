#!/bin/bash

for i in 2 4 8 12 16 24 32 48 64; do
	java -jar kmeans/build/libs/Kmeans.jar --inputPath Intermediate_Results/train_embeddings.csv \
	--centroidPath Intermediate_Results/train_embeddings.csv -k $i -n 25 \
	--outputScheme Intermediate_Results/k${i}_ -skip 1
	mv Intermediate_Results/k${i}_25/part-r-00000 Intermediate_Results/k${i}_dists
	rm -r Intermediate_Results/k${i}_[0-9]
	rm -r Intermediate_Results/k${i}_[0-9][0-9]
done

