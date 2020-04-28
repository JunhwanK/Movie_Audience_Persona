import argparse
import csv
import matplotlib.pyplot as plt


def main():
	#create argparer
	parser = argparse.ArgumentParser()
	parser.add_argument("--in_path", required=True, dest='in_path')
	
	#parse command line arguments
	args = parser.parse_args()

	lookup = {} #k --> (cost, id)
	with open(args.in_path) as in_file:
		csv_reader = csv.reader(in_file, delimiter=',')
		for row in csv_reader:
			i = int(row[0]) #id
			num_cent = int(row[1]) #num centroids
			cost = float(row[2])
			if num_cent in lookup and lookup[num_cent] > cost:
				lookup[num_cent] = cost
			else:
				lookup[num_cent] = cost

	items = sorted(list(lookup.items()))
	xs, ys = zip(*items)
	plt.plot(xs, ys)
	plt.title("Cost Over Different Number of Cluster")
	plt.xlabel("Num Cluster")
	plt.ylabel("Cost (sum of cosine distances)")
	plt.savefig("costs_plot.png")

if __name__ == "__main__":
	main()
