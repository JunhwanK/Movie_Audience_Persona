import argparse
import csv
import numpy as np


#program arugments:
# --persona for the csv of the ratings for that persona
# --output_path to specify the output file path
# --movie_names to csv of the movie id to movie name csv
# --treshold for a float or int value of threshold to output movie recommendations
def main():
	#create argparer
	parser = argparse.ArgumentParser()
	parser.add_argument("--persona", required=True, dest='persona')
	parser.add_argument("--movie_names", required=True, dest='movies')
	parser.add_argument("--threshold", dest='threshold', type=float)
	parser.add_argument("--output_path", required=True, dest='output_path')
	parser.add_argument("--num", dest='num', type=int)
	
	#parse command line arguments
	args = parser.parse_args()
	#read in movie id to movie name
	movie_names = {}
	skipped_header = False
	with open(args.movies) as in_file:
		csv_reader = csv.reader(in_file, quotechar='"')
		for row in csv_reader:
			if not skipped_header:
				skipped_header = True
				continue
			movie_names[int(row[0])] = row[1] #movie id, name
	#read in persona's expected movie ratings
	persona = np.genfromtxt(args.persona, delimiter=',')
	
	#scale expected ratings to [0,5] range
	persona -= np.min(persona)
	persona =  persona / np.max(persona) * 5

	#output index of recommended movies
	with open(args.output_path, 'w') as out_file:
		if args.threshold is not None:
			for i, rating in enumerate(persona):
				if rating > args.threshold:
					movie_name = movie_names[i+1] #1-indexing for movie id
					out_file.write('{},"{}",{}\n'.format(i+1, movie_name, rating)) #id, name, rating
		elif args.num is not None:
			ranking = np.argsort(persona)
			for i in range(args.num):
				ind = ranking[-1-i]
				movie_name = movie_names[ind+1] #1-indexing for movie id
				rating = persona[ind]
				out_file.write('{},"{}",{}\n'.format(i+1, movie_name, rating)) #id, name, rating

if __name__ == "__main__":
	main()
