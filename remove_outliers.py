import pandas as pd
import argparse

def main():
	#create argparer
	parser = argparse.ArgumentParser()
	parser.add_argument("--in_path", required=True, dest="in_path")
	parser.add_argument("--out_path", required=True, dest="out_path")
	#parse command line arguments
	args = parser.parse_args()

	#header names
	id_headers = ["cent_id", "point_id"]
	pt_headers = []
	for i in range(52):
		pt_headers.append("em{}".format(i))
	dt_headers = ["dist"]

	#read data
	df = pd.read_csv(args.in_path, names=id_headers+pt_headers+dt_headers)

	#for each cluster
	cent_ids = df["cent_id"].unique()
	for cid in cent_ids:
		cent_df = df[df["cent_id"] == cid] #view
		#calculate z-score
		df.loc[df["cent_id"] == cid, "z-score"] = (cent_df.dist - cent_df.dist.mean())/cent_df.dist.std(ddof=0)
	
	#remove outliers
	df = df[(df["z-score"] <= 3) | (df["z-score"] >= -3)]
	#create accumualator
	new_cents = pd.DataFrame(columns=pt_headers)
	#recompute centroids
	for cid in cent_ids:
		cent_df = df.loc[df["cent_id"] == cid, pt_headers] #view
		new_cents = new_cents.append(cent_df.mean(), ignore_index=True)

	new_cents.to_csv(args.out_path, header=False, index=False)


if __name__ == "__main__":
	main()
