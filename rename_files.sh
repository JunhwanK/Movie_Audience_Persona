#!/bin/bash

for i in 2 4 8 12 16 24 32 48 64; do
	mv Intermediate_Results/k${i}_final/part-r-00000 Intermediate_Results/k${i}_dists
done

