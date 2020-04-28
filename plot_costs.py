import argparse
import csv
import matplotlib.pyplot as plt


def main():
	#create argparer
	parser = argparse.ArgumentParser()
	parser.add_argument("--in_path", required=True, dest='in_path')
	
	#parse command line arguments
	args = parser.parse_args()

	xs = []
	ys = []
	with open(args.in_path) as in_file:
		csv_reader = csv.reader(in_file, delimiter=',')
		for row in csv_reader:
			xs.append(int(row[0])) #k num
			ys.append(float(row[1])) #cost

	plt.plot(xs, ys)
	plt.title("Cost Over Different Number of Cluster")
	plt.xlabel("Num Cluster")
	plt.ylabel("Cost (sum of cosine distances)")
	plt.savefig("costs_plot.png")

if __name__ == "__main__":
	main()
