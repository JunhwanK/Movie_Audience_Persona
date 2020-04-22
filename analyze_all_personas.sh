#!/bin/bash
#runs command for i = 0 to [first input argument]
i=0
NUM=$1
while [ $i -lt $NUM ]; do
	python3 analyze_persona.py --persona persona_ratings/persona_${i}.csv \
		--movie_names movies.csv --output_path persona_movie_selections/persona_${i}_movies.csv \
		--threshold 3.999
	echo "completed $i"
	i=$(expr $i + 1)
done
