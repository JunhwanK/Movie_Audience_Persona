import argparse
import pandas

def main():
	#create argparer
	parser = argparse.ArgumentParser()
	parser.add_argument("--input_format", required=True, dest='in_format')
	
	#parse command line arguments
	args = parser.parse_args()
	if args.in_format.find("{}") == "-1":
		print("input_format must include one {} for formatting")
		return

	ks = [2, 4, 8, 12, 16, 24, 28, 32, 36, 40, 44, 48, 52, 56, 64]
	costs = []
	actual_ks = []

	df_headers = ["cent_id", "point_id"]
	for i in range(52):
		df_headers.append("em{}".format(i))
	df_headers.append("dist")

	for i in ks:
		cost = 0
		file_name = args.in_format.format(i)
		with open(file_name) as in_file:
			for line in in_file:
				last_comma_ind = line.rfind(",")
				cost += float(line[last_comma_ind+1:])
			costs.append(cost)
		df = pandas.read_csv(file_name, names=df_headers, usecols=['cent_id'])
		actual_ks.append(len(df["cent_id"].unique()))

	with open("costs.csv", 'w') as out_file:
		for i,k,c in zip(ks, actual_ks, costs):
			out_file.write("{},{},{}\n".format(i,k,c))

if __name__ == "__main__":
	main()

