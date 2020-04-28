#!/bin/bash

cd kmeans
./gradlew clean jar
cd ..
rm -r Intermediate_Results/k${i}_[0-9]*

for i in 28 36 44 52 56; do
	java -jar kmeans/build/libs/Kmeans.jar --inputPath Intermediate_Results/train_embeddings.csv \
	--centroidPath Intermediate_Results/centroids_random.csv -k $i -n 25 \
	--outputScheme Intermediate_Results/k${i}_ -skip 1
	mv Intermediate_Results/k${i}_25/part-r-00000 Intermediate_Results/k${i}_dists.csv
	rm -r Intermediate_Results/k${i}_[0-9]*
done

