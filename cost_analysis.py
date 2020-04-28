import argparse

def main():
	#create argparer
	parser = argparse.ArgumentParser()
	parser.add_argument("--input_format", required=True, dest='in_format')
	
	#parse command line arguments
	args = parser.parse_args()
	if args.in_format.find("{}") == "-1":
		print("input_format must include one {} for formatting")
		return

	ks = [2, 4, 8, 12, 16, 24, 32, 40, 48, 64]
	costs = []

	for i in ks:
		cost = 0
		with open(args.in_format.format(i)) as in_file:
			for line in in_file:
				last_comma_ind = line.rfind(",")
				cost += float(line[last_comma_ind+1:])
			costs.append(cost)

	with open("costs.csv", 'w') as out_file:
		for k,c in zip(ks, costs):
			out_file.write("{},{}\n".format(k,c))

if __name__ == "__main__":
	main()

