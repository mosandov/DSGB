import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HadoopKMeans extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new HadoopKMeans(), args);

    System.exit(res);
  }

  @SuppressWarnings("deprecation")
  @Override
  public int run(String[] args) throws Exception {
    long t0 = System.nanoTime();

    Configuration conf = this.getConf();
    FileSystem hdfs = FileSystem.get(conf);
    //    String fileSystemURI = "";
    String fileSystemURI = hdfs.getUri().toString() + "/";

    int numberReducers = Integer.parseInt(args[0]);
    int dimension = Integer.parseInt(args[1]);
    int numberCenters = Integer.parseInt(args[2]);
    int maxIterations = Integer.parseInt(args[3]);
    double frequency = Double.parseDouble(args[4]);
    double error = Double.parseDouble(args[5]); // Error is not equal to epsilon from SGB.
    /*
     * Appending name of cluster HDFS to the input path name.
     * Data must reside on cluster HDFS.
     */
    //    String inputPath = args[6];
    String inputPath = fileSystemURI + args[6];

    int iteration = 0;
    /*
     * Global parameters being passed to the configuration object.
     */
    conf.setInt("dimension", dimension);
    conf.setInt("numberCenters", numberCenters);
    conf.setInt("iteration", iteration);
    conf.setDouble("frequency", frequency);

    /*
     * Temporary directory used to store initial sampled centers for running
     * KMeans.
     */
    //    String sampledCenters = "intial-centers";
    String sampledCenters = fileSystemURI + "intial-centers";

    /*
     * Directory used to store all computed centers.
     * The directory and its contents will be deleted at the
     * of this programs life for convenience.
     */
    //    String centersDirectory = "centers";
    String centersDirectory = fileSystemURI + "centers";

    /*
     * Single file that is the output of a one reducer mapreduce job.
     */
    String unitPartionFile = "/part-r-00000";

    /*
     * Directory used to temporarily store each iterations mapreduce job partition files.
     */
    //    String computedCentersJobDirectory = "computed_centers";
    String computedCentersJobDirectory = fileSystemURI + "computed_centers";

    String parameters =
        "Number of Reducers: "
            + numberReducers
            + "\nDimension: "
            + dimension
            + "\nNumber of Centers: "
            + numberCenters
            + "\nMax Number of Iterations: "
            + maxIterations
            + "\nError: "
            + error
            + "\nFrequency: "
            + frequency
            + "\nInputPath: "
            + inputPath
            + "\nOutputPath: "
            + centersDirectory;

    System.out.println(parameters);

    Job samplerJob = Job.getInstance(conf, "Sampler");
    samplerJob.setJarByClass(HadoopKMeans.class);
    samplerJob.setMapperClass(KMeansSplitSamplerMapper.class);
    samplerJob.setReducerClass(KMeansSplitSamplerReducer.class);
    samplerJob.setMapOutputKeyClass(LongWritable.class);
    samplerJob.setMapOutputValueClass(Text.class);
    samplerJob.setOutputFormatClass(TextOutputFormat.class);
    samplerJob.setOutputKeyClass(NullWritable.class);
    samplerJob.setOutputValueClass(Text.class);
    samplerJob.setPartitionerClass(HashPartitioner.class);
    samplerJob.setJarByClass(HadoopKMeans.class);

    FileInputFormat.addInputPath(samplerJob, new Path(inputPath));
    FileOutputFormat.setOutputPath(samplerJob, new Path(sampledCenters));

    /*
     * Task set to 1 to produce single partition file.
     */
    samplerJob.setNumReduceTasks(1);
    /*
     *  Exist status 1 if job failed, exit immediately.
     *  Directories are left undeleted.
     */
    if (!samplerJob.waitForCompletion(true)) {
      return 1;
    }
    /*
     * This directory should not exist at runtime.
     */
    hdfs.mkdirs(new Path(centersDirectory));

    /*
     * Rename/move initial sampled centers to directory dedicated to storing center files.
     */
    hdfs.rename(
        new Path(sampledCenters + unitPartionFile), new Path(centersDirectory + "/centers0.txt"));

    hdfs.delete(new Path(sampledCenters), true);

    /*
     * Number of centers identified by K-Means for a given iteration.
     */
    long numberCentersIdentified = 0L;

    boolean converged = false;
    double previousCost = Double.MAX_VALUE;
    double cost = 0.0;
    /*
     * The goal of K-Means is to reduce its Cost function also known as Residual Sum of Squares or RSS.
     */
    while (iteration < maxIterations && !converged) {

      /*
       * Hadoop boilerplate.
       */
      Job kMeansJob = Job.getInstance(conf);
      kMeansJob.setJobName("K-Means Job " + iteration);
      kMeansJob.setJarByClass(HadoopKMeans.class);
      kMeansJob.setMapperClass(KMeansMapper.class);
      kMeansJob.setReducerClass(KMeansReducer.class);
      kMeansJob.setMapOutputKeyClass(IntWritable.class);
      kMeansJob.setMapOutputValueClass(KMeansVector.class);
      kMeansJob.setOutputKeyClass(NullWritable.class);
      kMeansJob.setOutputValueClass(Text.class);
      kMeansJob.setPartitionerClass(HashPartitioner.class);
      kMeansJob.setNumReduceTasks(numberReducers);

      /*
       * Add previous/sampled centers to cache.
       */
      kMeansJob.addCacheFile(
          new URI(centersDirectory + "/centers" + iteration + ".txt#centers" + iteration + ".txt"));

      FileInputFormat.addInputPath(kMeansJob, new Path(inputPath));
      FileOutputFormat.setOutputPath(kMeansJob, new Path(computedCentersJobDirectory));
      /*
       *  Exist status 1 if job failed, exit immediately.
       *  Directories are left undeleted.
       */
      if (!kMeansJob.waitForCompletion(true)) {
        return 1;
      }

      /*
       * Take the multiple partitions files from the job output and merge them.
       * New merged file will appear in the directory named centers with the name of the job output directory i.e. computed_centers.
       *
       * This method of merging files is deprecated, but it's a better option than merging all the centers
       * to a single file using a map-reduce job.
       */
      FileUtil.copyMerge(
          hdfs,
          new Path(computedCentersJobDirectory),
          hdfs,
          new Path(centersDirectory),
          false,
          conf,
          "");
      /*
       * Delete the no longer needed job output directory. If not deleted, Hadoop will
       * throw a directory already exists exception when the next job iteration attempts to run.
       */
      hdfs.delete(new Path(computedCentersJobDirectory), true);

      /*
       * Rename the merged job file from computed_centers to centers_(i + 1).txt.
       */
      Path newestCentersFilePath =
          new Path(centersDirectory + "/centers" + (iteration + 1) + ".txt");
      hdfs.rename(new Path(centersDirectory + "/computed_centers"), newestCentersFilePath);

      /*
       * Open the newest centers file and read the cost/error associated with each center
       * to then compute the total RSE for this iteration.
       */
      InputStream inputStream = null;
      try {
        inputStream = hdfs.open(newestCentersFilePath);
        BufferedReader buffer = new BufferedReader(new InputStreamReader(inputStream));
        Iterator<String> bufferedLinesIter = buffer.lines().iterator();
        cost = 0;
        numberCentersIdentified = 0L;
        while (bufferedLinesIter.hasNext()) {
          String costString = bufferedLinesIter.next().split("\\s+")[0];
          cost += Double.parseDouble(costString);
          numberCentersIdentified++;
        }
      } finally {
        inputStream.close();
      }

      System.out.println("Number of centers: " + numberCentersIdentified);
      System.out.println("Iteration: " + iteration);
      System.out.println("Previous Cost: " + previousCost + " Cost: " + cost);
      /*
       * Test for convergence:
       * If the distance between the previous cost and current cost is less than epsilon, then
       * K-Means has converged.
       */
      if (Math.abs(previousCost - cost) < error) {
        System.out.println("K-Means has converged!");
        converged = true;
      }
      previousCost = cost;
      iteration++;
      conf.setInt("iteration", iteration);
    }

    long t1 = System.nanoTime();

    String date =
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")).toString();

    String executionTimeStr = "Execution time (ns): " + (t1 - t0);
    String numberCentersStr = "Clusters Identified: " + numberCentersIdentified;
    String finalCostStr = "Final Cost: " + previousCost;
    String iterationsStr = "Total Iterations: " + iteration;

    String summary =
        "Date_Time: "
            + date
            + "\n"
            + parameters
            + "\n"
            + iterationsStr
            + "\n"
            + finalCostStr
            + "\n"
            + numberCentersStr
            + "\n"
            + executionTimeStr;

    System.out.println("Summary: \n" + summary);

    String fileName = "HadoopKMeans_" + date;

    Path statsPath = new Path(hdfs.getUri() + "/stats/Stats_" + fileName + ".txt");
    FSDataOutputStream statsFile = hdfs.create(statsPath);
    statsFile.writeUTF(summary);
    statsFile.close();

    // Delete the output directory for convenience of submitting another experiment job.
    // hdfs.delete(new Path(centersDirectory), true);
    hdfs.close();
    return 0;
  }

  /*
   * Use of LongWritable as the write key was arbitrary.
   */
  public static class KMeansSplitSamplerMapper
      extends Mapper<LongWritable, Text, LongWritable, Text> {

    private double frequency;

    @Override
    public void setup(Context context) {
      frequency = context.getConfiguration().getDouble("frequency", -1.0);
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      if (ThreadLocalRandom.current().nextDouble() < frequency) {
        context.write(new LongWritable(1L), value);
      }
    }
  }

  public static class KMeansSplitSamplerReducer
      extends Reducer<LongWritable, Text, DoubleWritable, Text> {

    private int numberCenters;
    private int dimension;

    public void setup(Context context) {
      numberCenters = context.getConfiguration().getInt("numberCenters", -1);
      dimension = context.getConfiguration().getInt("dimension", -1);
    }

    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      /*
       * Only the K specified number of initial centers are returned, presuming enough entries were
       * polled.
       */
      Iterator<Text> valuesIterator = values.iterator();
      for (int count = 0; count < numberCenters && valuesIterator.hasNext(); count++) {

        String[] csvEntries = valuesIterator.next().toString().split(",");

        StringBuilder featureBuilder = new StringBuilder(2 * dimension - 1);

        /*
         * Removes ID and aggregation value information from string and only writes feature vector information.
         */
        for (int i = 0; i < dimension; i++) {
          if (i < dimension - 1) {
            featureBuilder.append(csvEntries[i + 2]).append(",");
          } else {
            featureBuilder.append(csvEntries[i + 2]);
          }
        }
        context.write(new DoubleWritable(0.0), new Text(featureBuilder.toString()));
      }
    }
  }

  public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, KMeansVector> {

    private int dimension;
    private int iteration;
    private double[][] centers;

    /**
     * Returns squared distance between two vectors/arrays.
     *
     * @param a Double array/vector
     * @param b Double array/vector
     * @return squared distance of given arrays.
     */
    private static double squaredDistance(double[] a, double[] b) {
      double difference;
      double squaredDistance = 0;
      for (int i = 0; i < a.length; i++) {
        difference = a[i] - b[i];
        squaredDistance += difference * difference;
      }
      return squaredDistance;
    }

    @Override
    public void setup(Context context) throws IOException {

      dimension = context.getConfiguration().getInt("dimension", -1);
      iteration = context.getConfiguration().getInt("iteration", -1);

      /*
       * Read centers from distributed cache and convert to a matrix.
       */
      centers =
          Files.lines(Paths.get("centers" + iteration + ".txt"))
              .map(
                  line -> {
                    String[] featureEntries = line.split("\\s+")[1].split(",");
                    double[] feature = new double[dimension];
                    for (int i = 0; i < dimension; i++) {
                      feature[i] = Double.parseDouble(featureEntries[i]);
                    }
                    return feature;
                  })
              .toArray(double[][]::new);

      System.out.println("Number of centers: " + centers.length);
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      /*
       * Convert CSV row entry into array/vector. The ID and aggregation values of the row are not
       * retained.
       */
      String[] csvEntries = value.toString().split(",");
      double[] feature = new double[dimension];
      for (int i = 0; i < dimension; i++) {
        feature[i] = Double.parseDouble(csvEntries[i + 2]);
      }
      double distance;
      double closestCenterDistance = Double.MAX_VALUE;
      int closestCenterIndex = 0;
      /*
       * Find the index (the K) to which this vector is closest to.
       */
      for (int i = 0; i < centers.length; i++) {
        distance = squaredDistance(feature, centers[i]);
        if (distance < closestCenterDistance) {
          closestCenterDistance = distance;
          closestCenterIndex = i;
        }
      }
      /*
       * Write the KMeansVector object with count initialized to 1.
       */
      context.write(
          new IntWritable(closestCenterIndex),
          new KMeansVector(1L, closestCenterDistance, feature));
    }
  }

  public static class KMeansReducer
      extends Reducer<IntWritable, KMeansVector, DoubleWritable, Text> {

    private int dimension;

    @Override
    public void setup(Context context) {
      dimension = context.getConfiguration().getInt("dimension", -1);
    }

    public void reduce(IntWritable key, Iterable<KMeansVector> values, Context context)
        throws IOException, InterruptedException {

      long totalCount = 0;
      double totalCost = 0;
      double[] sumsOnElements = new double[dimension];
      /*
       * Compute the total vector sum for vectors assigned to this centroid,
       * find the total cost, and total count of this group.
       *
       * The totalCount is used for the finding the
       */
      for (KMeansVector currentVector : values) {
        for (int i = 0; i < dimension; i++) {
          sumsOnElements[i] += currentVector.getFeature()[i];
        }
        totalCount += currentVector.getCount();
        totalCost += currentVector.getCost();
      }
      /*
       * Compute final centroid.
       */
      for (int i = 0; i < dimension; i++) {
        sumsOnElements[i] /= totalCount;
      }
      /*
       * Convert newly computed centroid into a CSV string representation.
       */
      StringBuilder centroidVectorBuilder = new StringBuilder(2 * dimension - 1);
      for (int i = 0; i < dimension; i++) {
        if (i < dimension - 1) {
          centroidVectorBuilder.append(String.valueOf(sumsOnElements[i])).append(",");
        } else {
          centroidVectorBuilder.append(String.valueOf(sumsOnElements[i]));
        }
      }
      /*
       * Report the total cost for computing this centroid via the key and newly computed center as the value.
       */
      context.write(new DoubleWritable(totalCost), new Text(centroidVectorBuilder.toString()));
    }
  }
}
