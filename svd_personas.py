import argparse
import csv
import numpy as np

#program takes in an arugment --VT_matrix to the 90% energy V^T matrix of SVD
# and --output_path to specify the output file header name
# e.g. "--output_path persona" will output files persona_#.csv per persona
def main():
	#create argparer
	parser = argparse.ArgumentParser()
	parser.add_argument("--VT_matrix", required=True, dest='VT_matrix')
	parser.add_argument("--centroids", required=True, dest='centroids')
	parser.add_argument("--output_path", required=True, dest='output_path')
	
	#parse command line arguments
	args = parser.parse_args()

	VT_90 = np.genfromtxt(args.VT_matrix, delimiter=',')
	centroids = np.genfromtxt(args.centroids, delimiter=',')

	#calculate ratings
	ratings = centroids.dot(VT_90)
	for i, row in enumerate(ratings):
		np.savetxt(args.output_path + "_{}.csv".format(i), row) 

if __name__ == "__main__":
	main()
