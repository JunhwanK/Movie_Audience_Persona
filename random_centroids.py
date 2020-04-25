import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in_path", required=True, dest='in_path')
    parser.add_argument("--out_path", required=True, dest='out_path')
    parser.add_argument("--num_cent", required=True, type=int, dest='num_cent')
    
    #parse command line arguments
    args = parser.parse_args()
    assert(args.num_cent >= 1)

    f = open(args.in_path, 'r')
    contents = f.readlines()
    f.close()
    
    interval = len(contents) // args.num_cent
    assert(interval >= 1)

    count = 0
    with open (args.out_path, 'w') as out_file:
        for i in range(0, len(contents), interval):
             out_file.write(contents[i])
             count += 1
    print("Num centroids output:{}".format(count))

if __name__== "__main__":
    main()
