import csv

#ratings.csv must be in the same directory

skipped_header = False
with open("ratings.csv") as in_file:
    csvreader = csv.reader(in_file)
	#read each row of ratings.csv (userId,movieId,rating,timestamp)
    with open("ratings_train.csv", 'w') as train_out:
        with open("ratings_test.csv", 'w') as test_out:
            for row in csvreader:
                if not skipped_header:
                    skipped_header = True
                    continue
                elif int(row[0]) <= 146541:
                    train_out.write(",".join(row[:-1]))
                    train_out.write("\n")
                else: #rest of the data (16000 of them)
                    test_out.write(",".join(row[:-1]))
                    test_out.write("\n")
