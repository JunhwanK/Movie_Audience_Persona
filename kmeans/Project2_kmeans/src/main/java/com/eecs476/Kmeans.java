package com.eecs476;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.util.*;
import java.text.*;
import java.util.Arrays;
import java.util.ArrayList;
import java.lang.*;



public class Kmeans {
    private static boolean TO_PRINT = false;
    private static String inputPath;
    private static String centroidPath;
    private static String outputScheme;
    private static int norm;
    private static int k;
    private static int n;
    private static int numFeatures;
    private final static IntWritable one = new IntWritable(1);

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        for (int i = 0; i < args.length; ++i) {
            if (args[i].equals("--inputPath")) {
                inputPath = args[++i];
                //System.out.println(inputPath + "\n");
            } else if (args[i].equals("--centroidPath")) {
                centroidPath = args[++i];
                //System.out.println(centroidPath + "\n");
            } else if (args[i].equals("--norm")) {
                norm = Integer.parseInt(args[++i]);
                //System.out.println(norm + "\n");
            } else if (args[i].equals("-k")) {
                k = Integer.parseInt(args[++i]);
                //System.out.println(k + "\n");
            } else if (args[i].equals("-n")) {
                n = Integer.parseInt(args[++i]);
                //System.out.println(n + "\n");
            } else if (args[i].equals("--outputScheme")) {
                outputScheme = args[++i];
                //System.out.println(outputScheme + "\n");
            } else {
                throw new IllegalArgumentException("Illegal cmd line arguement");
            }
        }
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        conf.set("mapreduce.job.queuename", "eecs476");         // required for this to work on GreatLakes
        conf.setInt("num_records", 5);

        // set outputPath
        Path outputPath = new Path(Path.CUR_DIR, outputScheme);
        //System.out.println(outputPath + "\n");
/*
        // Read centroid file and initialize k centroids to a global ArrayList, centroids
        BufferedReader reader = new BufferedReader(new FileReader(centroidPath));
        String line = null;
        while ((line = reader.readLine()) != null) {
            // read each centroid in, convert to list<string> and save
            String[] centroidArr = line.split(",", 0);
            ArrayList<Float> centroidList = new ArrayList<Float>();
            int count = 0;
            for (String coord : centroidArr) {
                // convert to float and append to list
                centroidList.add(Float.parseFloat(coord));
                count++;
            }
            // add individual centroidlist to list of centroids
            centroids.add(centroidList);
            numFeatures = count;
            //System.out.println("Num centroids in randomC.txt: ");
            //System.out.println(centroids.size());
        }
        reader.close();
*/

        // run algorithm n times aka n jobs, from 1 to n eg n=3: 1 2 3 not 0 1 2
        for (int i = 1; i < n + 1; i++) {
            if (i == n) {
                TO_PRINT = true;
            }
            //System.out.println("ROUND NUMBER: " + String.valueOf(i));
            String jobname = "j" + String.valueOf(i);
            Job j1 = Job.getInstance(conf, jobname);
            if (i == 1) {
                // For first iteration, cache centroids file passed in as arguments aka original centroids
                j1.addCacheFile(new Path(centroidPath).toUri());
            } else {
                // For subsequent iterations, cache previous reducer's output aka new centroids
                j1.addCacheFile(new Path(outputPath.toString() + String.valueOf(i-1), "part-r-00000").toUri());
            }

            j1.setJarByClass(Kmeans.class);
            j1.setMapperClass(KmeansMapper1.class);
            j1.setReducerClass(KmeansReducer1.class);

            // output key is list of floats(centroid), val is float (distance from this point to centroid)
            j1.setMapOutputKeyClass(Text.class);
            j1.setMapOutputValueClass(Text.class);
            // Reducer output = list of floats(centroid)
            j1.setOutputKeyClass(Text.class);
            j1.setOutputValueClass(Text.class);

            // Check if the input output formats need changing
            j1.setInputFormatClass(TextInputFormat.class);
            j1.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(j1, new Path(inputPath));
            FileOutputFormat.setOutputPath(j1, new Path(outputPath.toString() + String.valueOf(i)));
            j1.waitForCompletion(true);
        }


    }// end main
    //---------------------------------------------------------------------------------------------------
// Mapper1

    // template arguments are <input key, input value, output key, output value>
    public static class KmeansMapper1 extends Mapper<LongWritable, Text, Text, Text> {

        private static float l1norm(ArrayList<Float> p1, ArrayList<Float> p2) {
            float sum = (float)0.0;
            //System.out.println("Point 1: ");
            //System.out.println(p1);
            //System.out.println("Point 2: ");
            //System.out.println(p2);
            for (int i = 0; i < p1.size(); ++i) {
                //System.out.println(i);
                sum += Math.abs(p1.get(i) - p2.get(i));
            }
            return sum;
        }

        private static float l2norm(ArrayList<Float> p1, ArrayList<Float> p2) {
            float sumsq = (float)0.0;
            for (int i = 0; i < p1.size(); ++i) {
                sumsq += (p1.get(i) - p2.get(i)) * (p1.get(i) - p2.get(i));
            }
            return (float)Math.sqrt(sumsq);
        }

        private static float magnitude(ArrayList<Float> coords) {
            float sumsq = (float)0.0;
            for (float coord : coords) {
                sumsq += coord * coord;
            }
            return (float)Math.sqrt(sumsq);
        }

        private static List<ArrayList<Float>> centroids = new ArrayList<ArrayList<Float>>();

        // In setup, read centroid file or previous reducer's output from file directly
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            //Get the URI of the reducer output for all items passing threshold in cache
            URI[] cacheFile = context.getCacheFiles();

            // Get cached filename
            String filename = null;
            int lastindex = cacheFile[0].toString().lastIndexOf('/');
            if (lastindex != -1) {
                filename = cacheFile[0].toString().substring(lastindex + 1, cacheFile[0].toString().length());
            } else {
                filename = cacheFile[0].toString();
            }

            centroids.clear();
            //Read the content of the List using a Buffered reader
            BufferedReader reader = new BufferedReader(new FileReader(filename));
            String line = null;
            while ((line = reader.readLine()) != null) {
                // read each centroid in, convert to list<string> and save
                String[] centroidArr = line.split(",", 0);
                ArrayList<Float> centroidList = new ArrayList<Float>();
                int count = 0;
                for (String coord : centroidArr) {
                    // convert to float and append to list
                    centroidList.add(Float.parseFloat(coord));
                    count++;
                }
                // add individual centroidlist to list of centroids
                centroids.add(centroidList);
                numFeatures = count;
                //System.out.println("Num centroids now and centroids cached: ");
                //System.out.println(centroids.size());
                //System.out.println(centroids);

            }
            reader.close();
        }

        // Input arguments must match the first two template arguments
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // convert text to string for easy manipulation
            String inputTextasString = value.toString();
            //System.out.println("mapper input: " + inputTextasString);
            // split each line in inputfile into tokens
            String[] idxdataPointStr = inputTextasString.split(",", 0);
            // TODO: SEPARATE INDEX AND COORDS
            String[] dataPointStr = Arrays.copyOfRange(idxdataPointStr, 1, idxdataPointStr.length);
            ArrayList<Float> dataPoint = new ArrayList<Float>();
            for (String point : dataPointStr) {
                ////System.out.println("Coord in mapper input data: " + point);
                dataPoint.add(Float.parseFloat(point));
            }

            // this is the current nearest centroid
            ArrayList<Float> nearestCentroid = new ArrayList<Float>();
            float nearestDistance = Float.POSITIVE_INFINITY;
            // loop through list of centroids and calculate distance from each centroid to this point
            int i = 0;
            for (ArrayList<Float> centroid : centroids) {
                // calculate distance
                //System.out.println("centroid: ");
                //System.out.println(centroid);
                if (norm == 1) {
                    //System.out.println("Norm 1, comparing ");
                    float distance = l1norm(centroid, dataPoint);
                    //System.out.println(distance);
                    if (Float.compare(distance, nearestDistance) == 0) {
                        //System.out.println("centroids with same distance to this point found");
                        if (magnitude(centroid) < magnitude(nearestCentroid)) {
                            nearestDistance = distance;
                            nearestCentroid = centroid;
                        }
                    }
                    else if (Float.compare(distance, nearestDistance) < 0) {
                        //System.out.println("Nearer centroid found");
                        nearestDistance = distance;
                        nearestCentroid = centroid;
                    }
                } else {
                    float distance = l2norm(centroid, dataPoint);
                    if (Float.compare(distance, nearestDistance) == 0) {
                        //System.out.println("centroids with same distance to this point found");
                        if (magnitude(centroid) < magnitude(nearestCentroid)) {
                            nearestDistance = distance;
                            nearestCentroid = centroid;
                        }
                    }
                    else if (Float.compare(distance, nearestDistance) < 0) {
                        nearestDistance = distance;
                        nearestCentroid = centroid;
                    }
                }

            } // end loop of all centroids

            // Now we have best centroid and its distance to this point, output
            // Convert centroid aka list of floats to list of strings, join with comma
            //System.out.println("Distance to nearest centroid for this round and nearest centroid: ");
            // ONLY PRINT IF THIS IS THE FINAL ROUND.
            //System.out.println(nearestCentroid);
            List<String> centroidStrList = new ArrayList<String>();
            for (float elt : nearestCentroid) {
                centroidStrList.add(Float.toString(elt));
            }
            String centroidStr = String.join(",", centroidStrList);
            Text keyAsText = new Text(centroidStr);
            // pass value back as value because it already represents this current point being evaluated
            context.write(keyAsText, value);
        }
    }

    //---------------------------------------------------------------------------------------------------
    // Reducer
// template arguments are <input key, input value, output key, output value>
    // Writes new centroid which is a list of coords to global centroids list
    public static class KmeansReducer1 extends Reducer<Text, Text, Text, Text> {

        private static float l2norm(ArrayList<Float> p1, List<Float> p2) {
            float sumsq = (float)0.0;
            for (int i = 0; i < p1.size(); ++i) {
                sumsq += (p1.get(i) - p2.get(i)) * (p1.get(i) - p2.get(i));
            }
            return (float)Math.sqrt(sumsq);
        }

        // input will be <currentCentroid, iterable of pts(lists of coords) with nearest centroid = currentCentroid>
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Calculate new centroid and update global centroids list for next round
            List<Float> sumPoint = new ArrayList<Float>();
            for (int x = 0; x < numFeatures; ++x) {
                // initialize sumPoint to all 0 coords
                sumPoint.add((float)0.0);
            }
            int count = 0;
            //System.out.println("Reducing for mean of all points closest to this old centroid: ");
            //System.out.println(key);
            // Sum up all coords belonging to new centroid
            ArrayList<ArrayList<Float>> these_points = new ArrayList<ArrayList<Float>>();

            for (Text p : values) {
                ArrayList<Float> this_point = new ArrayList<Float>();
                // Convert this point into list of coords
                String inputTextasString = p.toString();
                //System.out.println("One of points used to calculate mean: ");
                //System.out.println(inputTextasString);
                // TODO: SEPARATE INDEX AND COORDS
                String[] idxdataPointStr = inputTextasString.split(",", 0);
                this_point.add(Float.parseFloat(idxdataPointStr[0]));
                String[] dataPointStr = Arrays.copyOfRange(idxdataPointStr, 1, idxdataPointStr.length);
                List<Float> dataPoint = new ArrayList<Float>();
                for (int i = 0; i < dataPointStr.length; ++i) {
                    float thisCoord = Float.parseFloat(dataPointStr[i]);
                    this_point.add(thisCoord);
                    sumPoint.set(i, sumPoint.get(i) + thisCoord) ;
                }
                these_points.add(this_point);
                count++;
            }
            ArrayList<Float> newCentroid = new ArrayList<Float>();
            String newCentroidStrComma = "";
            for (int i = 0; i < sumPoint.size(); ++i) {
                float newCoord = sumPoint.get(i) / count;
                newCentroid.add(newCoord);
                newCentroidStrComma += Float.toString(newCoord) + ",";
            }
            // remove last comma in string of newcentroid
            String newCentroidStr = newCentroidStrComma.substring(0, newCentroidStrComma.length() - 1);
            //System.out.println("NEW CENTROID: ");
            //System.out.println(newCentroidStr);

            // Output list of (point index, distance to its centroid)
            if (TO_PRINT == true) {
                String all_idx_distance = "";
                for (ArrayList<Float> this_p: these_points) {
                    Float distance = l2norm(newCentroid, this_p.subList(1,this_p.size()));
                    int idx = (this_p.get(0)).intValue();
                    all_idx_distance += Integer.toString(idx) + "," + Float.toString(distance) + "\n";
                }
                all_idx_distance += "\n";
                context.write(new Text(all_idx_distance), null);

                /*for (Text trythis : values) {
                    // Convert this point into list of coords
                    String inputTextasString = trythis.toString();
                    //System.out.println("One of points used to calculate mean: ");
                    //System.out.println(inputTextasString);
                    // TODO: SEPARATE INDEX AND COORDS
                    String[] idxdataPointStr = inputTextasString.split(",", 0);
                    String idx = idxdataPointStr[0];
                    String[] dataPointStr = Arrays.copyOfRange(idxdataPointStr, 1, idxdataPointStr.length);
                    ArrayList<Float> dataPoint = new ArrayList<Float>();
                    for (int i = 0; i < dataPointStr.length; ++i) {
                        float thisCoord = Float.parseFloat(dataPointStr[i]);
                        dataPoint.add(thisCoord);
                    }
                    Float distance = l2norm(newCentroid, dataPoint);
                    //all_idx_distance += idx + "," + Float.toString(distance) + "\n";
                    //String this_entry = idx + "," + Float.toString(distance) + "\n";
                    all_idx_distance += " la ";
                }
                // remove last newline in string of all idx and distances
                //assert(false);
                //String all_idx_distance_final = all_idx_distance.substring(0, all_idx_distance.length() - 1);
                all_idx_distance += " boo";
                context.write(new Text(all_idx_distance), null);*/

            } else {
                // output new centroid to be used as cache file for next round.
                context.write(new Text(newCentroidStr), null);
            }


        }
    }

}

