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
    private static String inputPath;
    private static String centroidPath;
    private static String outputScheme;
    private static int k;
    private static int n;
    private static int skip;
    private static int dimen;
    private final static IntWritable one = new IntWritable(1);

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        for (int i = 0; i < args.length; ++i) {
            if (args[i].equals("--inputPath")) {
                inputPath = args[++i];
            } else if (args[i].equals("--centroidPath")) {
                centroidPath = args[++i];
            } else if (args[i].equals("-k")) {
                k = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-n")) {
                n = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-skip")) {
                skip = Integer.parseInt(args[++i]);
            } else if (args[i].equals("--outputScheme")) {
                outputScheme = args[++i];
            } else {
                throw new IllegalArgumentException("Illegal cmd line arguement");
            }
        }
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        conf.set("mapreduce.job.queuename", "eecs476");         // required for this to work on GreatLakes
        conf.setInt("num_cluster", k);
        conf.setInt("skip", skip);

        // set outputPath
        Path outputPath = new Path(Path.CUR_DIR, outputScheme);

        // run algorithm n times aka n jobs, from 1 to n eg n=3: 1 2 3 not 0 1 2
        for (int i = 1; i < n + 1; i++) {
            if (i == n) {
                conf.setBoolean("last", true);
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

            j1.setMapOutputKeyClass(Text.class);
            j1.setMapOutputValueClass(Text.class);
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

        private static int n_centroids;
        private static int dimen;
        private static int skip;
        private static List<ArrayList<Float>> centroids = new ArrayList<ArrayList<Float>>();
        private static ArrayList<Float> cent_magnitudes = new ArrayList<Float>();

        private static float cos_dist(ArrayList<Float> cent, ArrayList<Float> point, int cent_idx, int skip) {
            assert(cent.size() == point.size());
            float dot_prod = 0;
            for (int i = skip; i < p1.size(); ++i) {
                dot_prod += cent.get(i) * point.get(i);
            }
            return dot_prod / cent_magnitudes.get(cent_idx) / magnitude(point, skip);
        }

        private static float magnitude(ArrayList<Float> coords, int skip) {
            float sumsq = 0;
            for (int i = skip; i < coords.size(); ++i) {
                sumsq += coords.get(i) * coords.get(i);
            }
            return Math.sqrt(sumsq);
        }

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


            int k = conf.getInt("num_cluster", 1);
            skip = conf.getInt("skip", 0);
            int cent_count = 0;
            centroids.clear();
            //Read the content of the List using a Buffered reader
            BufferedReader reader = new BufferedReader(new FileReader(filename));
            String line = null;
            while ((line = reader.readLine()) != null) {
                // read each centroid in, convert to list<string> and save
                String[] centroidArr = line.split(",", 0);
                ArrayList<Float> centroidList = new ArrayList<Float>();
                for (String coord : centroidArr) {
                    // convert to float and append to list
                    centroidList.add(Float.parseFloat(coord));
                }
                // add individual centroidlist to list of centroids
                centroids.add(centroidList);
                if (++cent_count == k) {
                    break;
                }
            }
            dimen = centroids[0].size();
            n_centroids = centroids.size();
            reader.close();

            cent_magnitudes.clear();
            for (int i = 0; i < n_centroids; ++i) {
                cent_magnitudes.add(magnitude(centroids.get(i), skip));
            }
        }

        // Input arguments must match the first two template arguments
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //create datapoint
            ArrayList<Float> dataPoint = new ArrayList<Float>(dimen);
            //read datapoint
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            while (itr.hasMoreTokens()) {
                float elem = new Float(itr.nextToken());
                dataPoint.add(elem);           
            }

            // this is the current nearest centroid
            int closest_cent = 0;
            float nearestDistance = Float.POSITIVE_INFINITY;
            for(int i = 0; i < n_centroids; ++i) {
                float dist = cos_dist(centroids.get(i), dataPoint, i, skip);
                if (Float.compare(dist, nearestDistance) == 0) {
                    //System.out.println("centroids with same distance to this point found");
                    if (magnitude(centroid, skip) < magnitude(centroids.get(closest_cent), skip)) {
                        nearestDistance = dist;
                        closest_cent = i;
                    }
                }
                else if (Float.compare(dist, nearestDistance) < 0) {
                    //System.out.println("Nearer centroid found");
                    nearestDistance = dist;
                    closest_cent = i;
                }
            }

            String centroidStr = centroids.get(closest_cent).stream()
                .map(i -> i.toString())
                .collect(Collectors.joining(","));
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

        private static float cos_dist(ArrayList<Float> p1, ArrayList<Float> p2, int skip) {
            assert(p1.size() == p2.size());
            float dot_prod = 0;
            for (int i = skip; i < p1.size(); ++i) {
                dot_prod += p1.get(i) * p2.get(i);
            }
            return dot_prod / magnitude(p1, skip) / magnitude(p2, skip);
        }

        private static float magnitude(ArrayList<Float> coords, int skip) {
            float sumsq = 0;
            for (int i = skip; i < coords.size(); ++i) {
                sumsq += coords.get(i) * coords.get(i);
            }
            return Math.sqrt(sumsq);
        }

        // input will be <currentCentroid, iterable of pts(lists of coords) with nearest centroid = currentCentroid>
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int skip = conf.getInt("skip", 0);
            boolean is_last = conf.getBool("last", false);
            ArrayList<ArrayList<Float>> points = new ArrayList<ArrayList<Float>>();

            Iterator<Text> itr = values.iterator();
            //create accumulator
            List<Float> centroid = new ArrayList<Float>();
            //read first point
            Text val = itr.next();
            StringTokenizer str_itr = new StringTokenizer(val.toString(), ",");
            while (str_itr.hasMoreTokens()) {
                float elem = Float.parse(str_itr.nextToken());
                centroid.add(elem);
            }

            if (is_last) {
                points.add(centroid.clone()); //shallow copy, but doesn't matter
            }

            float cent_id = Float.parse(StringTokenizer(key.toString(),",").nextToken());
            centroid.set(0, cent_id);

            int count = 1;
            //read rest of the values
            while (itr.hasNext()) {
                count += 1;

                val = itr.next();
                str_itr = new StringTokenizer(val.toString(), ",");
                int i = 0;
                ArrayList<Float> point = new ArrayList<Float>();
                while (str_itr.hasMoreTokens()) {
                    if (i == 0) {
                        str_itr.nextToken();
                        ++i;
                        continue;
                    }
                    float elem = Float.parse(str_itr.nextToken());
                    centroid.set(i, centroid.get(i)+elem);
                    if (is_last) {
                        point.add(elem);
                    }
                    ++i;
                }
                if (is_last) {
                    points.add(point);
                }
            }

            //calc centroid
            int dimen = centroid.size();
            for (int i = skip; i < dimen; ++i) {
                centroid.set(i, centroid.get(i)/count);
            }

            // Output list of (point index, distance to its centroid)
            if (is_last) {
                String output = Float.toString(centroid.get(0)) + ",";
                for (ArrayList<Float> point: points) {
                    float distance = cos_dist(centroid, point, skip);
                    String point_str = point.stream().map(i -> i.toString())
                                            .collect(Collectors.joining(","))
                    output += point_str + "," + Float.toString(distance) + "\n";
                }
                //cent_id,point_id,pt_embedding,distance
                context.write(new Text(output), null);

            } else {
                // output new centroid to be used as cache file for next round.
                String cent_str = centroid.stream().map(i -> i.toString())
                                            .collect(Collectors.joining(","))
                context.write(new Text(cent_str), null);
            }
        }
    }

}

