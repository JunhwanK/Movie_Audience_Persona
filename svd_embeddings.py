import argparse
import csv
import numpy as np
from scipy.sparse import csr_matrix
from scipy.sparse.linalg import svds, aslinearoperator
from scipy.linalg.interpolative import estimate_rank
#https://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.csr_matrix.html
#https://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.linalg.svds.html


#program takes in an arugment --matrix_data to the ratings.csv
# and --output_path to specify the output file header name
# e.g. "--output_path out" will output files "out_VT" and "out_embeddings"
#rating.csv format: userId,movieId,rating,timestamp
def main():
	#create argparer
	parser = argparse.ArgumentParser()
	parser.add_argument("--matrix_data", required=True, dest='matrix_data')
	parser.add_argument("--output_path", required=True, dest='output_path')
	
	#parse command line arguments
	args = parser.parse_args()

	#read csv file
	"""
	#it is faster to read line by line than to create numpy matrix
	data = np.genfromtxt(args.matrix_data, delimiter=',')
	row_ind = data[:,[0]].flatten().astype(int) - 1 #userId (1-indexing to 0-indexing)
	col_ind = data[:,[1]].flatten().astype(int) - 1 #movieId (1-indexing to 0-indexing)
	data = data[:,[2]].flatten().astype(int) #rating
	"""
	row_ind = []
	col_ind = []
	data =[]
	with open(args.matrix_data) as mat_in:
		csvreader = csv.reader(mat_in)
		#read each row of ratings.csv (userId,movieId,rating,timestamp)
		for row in csvreader:
			row_ind.append(int(row[0])-1) #userId (1-indexing to 0-indexing)
			col_ind.append(int(row[1])-1) #movieId (1-indexing to 0-indexing)
			data.append(float(row[2])) #rating

	#create sparse matrix
	matrix = csr_matrix((data, (row_ind,col_ind)))
	
	print(matrix.get_shape())

	#est_rank = estimate_rank(aslinearoperator(matrix), 0.005)
	#print(est_rank)
	#return

	#Singular value decomposition
	U, S, VT = svds(matrix, k=100, return_singular_vectors="vh")

	#sort in descending
	S = S[::-1]

	#find number of signular values to retain for 90% > power
	total_power = np.sum(S**2)
	num_retain = 0
	#accumulate power until reach over 90%
	power = 0.0
	num_retain = 0
	num_zero = 0
	np.savetxt(args.output_path + "_s_values.csv", S)
	for i, sing_val in enumerate(S):
		if sing_val == 0:
			num_zero += 1
			continue
		power += sing_val**2
		num_retain += 1
		if power / total_power >= 0.9:
			print("Power % retained: {}".format(power / total_power))
			break
	
	print("Num_retain: {}".format(num_retain))
	
	#extract non-zero singular value rows, reverse row order, retain fewer rows
	VT_90 = VT[:len(VT)-num_zero][::-1][:num_retain]

	#output 90% power V^T matrix
	np.savetxt(args.output_path + "_VT.csv", VT_90, delimiter=',')

	V_90 = np.transpose(VT_90)

	#calculate embeddings
	embeddings = matrix.dot(V_90)
	usr_ind = np.arange(len(embeddings)).astype(int) + 1
	#save to csv: user_id (back to 1-indexing), embedding
	np.savetxt(args.output_path + "_embeddings.csv",
			np.insert(embeddings, 0, usr_ind, axis=1),
			fmt = ','.join(['%i'] + ['%.18e']*num_retain)) 

if __name__ == "__main__":
	main()
