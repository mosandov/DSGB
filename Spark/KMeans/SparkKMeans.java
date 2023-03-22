import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.DoubleAccumulator;
import scala.Tuple2;

public class SparkKMeans {
  public static void main(String[] args) throws IOException {

	// Time stamp for counter
    long t0 = System.nanoTime();

    SparkConf conf = new SparkConf().setAppName("SparkKMeans");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    jsc.setLogLevel("WARN");

    /*
     * Establishing a connection to HDFS.
     */
    Configuration hadoopConf = jsc.hadoopConfiguration();
    FileSystem hdfs = FileSystem.get(hadoopConf);

    /*
     * Taking in the parameters passed via args.
     */
    int splits = Integer.parseInt(args[0]);
    int dimension = Integer.parseInt(args[1]);
    int numCenters = Integer.parseInt(args[2]);
    int maxIterations = Integer.parseInt(args[3]);
    double error = Double.parseDouble(args[4]); // Error is not equal to epsilon from SGB.
    long seed = Long.parseLong(args[5]);
    String pathToInput = args[6];
    String pathToOutput = args[7];

    /*
     * Appending name of cluster HDFS to path names.
     * Data must reside on cluster HDFS.
     */
    // pathToInput = hdfs.getUri() + "/" + pathToInput;
    // pathToOutput = hdfs.getUri() + "/" + pathToOutput;

    String parameters =
        "Splits: "
            + splits
            + "\nDimension: "
            + dimension
            + "\nNumber of Centers: "
            + numCenters
            + "\nMax Number of Iterations: "
            + maxIterations
            + "\nError: "
            + error
            + "\nSeed: "
            + seed
            + "\nInput Path: "
            + pathToInput
            + "\nOutput Path: "
            + pathToOutput;

    System.out.println(parameters);

    /*
     * Read the data file and convert the vector entries into arrays.
     * RDD is cached as K-means is an iterative algorithm that benefits from caching.
     */
    JavaRDD<double[]> featuresRDD =
        jsc.textFile(pathToInput, splits)
            .map(
                csvRow -> {
                  String[] csvEntries = csvRow.split(",");
                  double[] feature = new double[dimension];
                  for (int i = 0; i < dimension; i++) {
                    feature[i] = Double.parseDouble(csvEntries[i + 2]);
                  }
                  return feature;
                })
            .cache();
    /*
     * Retrieve a sample of vectors that function as the initial centroids of K-means.
     */
    double[][] centers =
        featuresRDD.takeSample(false, numCenters, seed).stream().toArray(double[][]::new);

    DoubleAccumulator costAccumulator = jsc.sc().doubleAccumulator("Cost");
    double cost;
    double previousCost = Double.MAX_VALUE;
    int iteration = 0;
    boolean converged = false;
    while (iteration < maxIterations && !converged) {

      final Broadcast<double[][]> bcCenters = jsc.broadcast(centers);
      centers =
          featuresRDD
              .mapPartitionsToPair(
                  featuresIterator ->
                      countsAndSumsPerPartition(featuresIterator, bcCenters, costAccumulator))
              .reduceByKey(
                  (a, b) ->
                      new Tuple2<>(
                          addVectors(a._1, b._1),
                          a._2 + b._2)) // Add the various sums and counts from different partitions
              .map(t -> vectorAverage(t._2._1, t._2._2)) // Perform the centroid averaging
              .collect()
              .stream()
              .toArray(double[][]::new); // Return the results as a type double martix

      // Retrieve the total sum of the Within Sum Squared Error
      cost = costAccumulator.sum();

      System.out.println("Iteration: " + iteration);
      System.out.println("Number of centers: " + centers.length);
      System.out.println("Previous Cost: " + previousCost + " Cost: " + cost);

      /*
       * Conditions for convergence. If the difference in current cost and the previous cost is
       * within a certain threshold, K-means has converged to a local minimum.
       */
      if (Math.abs(previousCost - cost) < error) {
        converged = true;
        System.out.println("K-Means has converged!");
      }
      previousCost = cost;
      costAccumulator.reset();
      iteration++;
    }

    /*
     * Write final set of centers to HDFS.
     * Note: Not required but feels necessary for a fair comparison.
     */
    ArrayList<double[]> centersList = new ArrayList<>(centers.length);
    Arrays.stream(centers).forEach(centersList::add);

    /*
     * Proceeding code prepares and writes a summary file of this algorithms execution.
     */
    String date =
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")).toString();

    /*
     * The string fileName is actually used as a directory name when spark writes the output.
     */
    String fileName = "SparkKMeans_" + date;
    jsc.parallelize(centersList).saveAsTextFile(pathToOutput + "/" + fileName);

    long t1 = System.nanoTime();

    String executionTimeStr = "Execution time (ns): " + (t1 - t0);
    String numberCentersStr = "Clusters Identified: " + centers.length;
    String finalCostStr = "Final Cost: " + previousCost;
    String iterationsStr = "Total Iterations: " + iteration;

    /*
     * writeUTF throws away the first line for some reason.
     * Easiest solution was to add a new line character at the beginning of the string.
     */
    String summary =
        "\nDate_Time: "
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

    System.out.println("Summary:" + summary);

    Path statsPath = new Path(hdfs.getUri() + "/stats/Stats_" + fileName);
    FSDataOutputStream statsFile = hdfs.create(statsPath);
    statsFile.writeUTF(summary);
    statsFile.close();
    // Delete the output of this job
    // hdfs.delete(new Path(pathToOutput), true);
    jsc.close();
  }

  /**
   * Primary method of K-means that finds the closest centroid for each point, tracks the number of
   * points assigned to a centroid, tracks the Within Sum Squared Error
   *
   * @param featuresIterator iterator of all the feature vectors stored in the given partition
   * @param bcCenters centroids of this iteration
   * @param costAccumulator accumulator that tracks the running Within Sum Squared Error
   * @return returns the sum of vectors assigned to each centroid and number of vectors assigned to
   *     each centroid
   */
  private static Iterator<Tuple2<Integer, Tuple2<double[], Long>>> countsAndSumsPerPartition(
      Iterator<double[]> featuresIterator,
      Broadcast<double[][]> bcCenters,
      DoubleAccumulator costAccumulator) {

    double[][] centers = bcCenters.value();
    double[][] clusterSums = new double[centers.length][];
    long[] clusterCounts = new long[centers.length];

    double[] currentFeature;
    double distance;
    double closestCenterDistance;
    int closestCenterIndex;
    /*
     * Initialize each double array in the clusterSums matrix.
     * This will fail if K < 1 for some reason.
     */
    for (int i = 0; i < centers.length; i++) {
      clusterSums[i] = new double[centers[0].length];
    }
    while (featuresIterator.hasNext()) {
      currentFeature = featuresIterator.next();
      closestCenterIndex = 0;
      closestCenterDistance = Double.MAX_VALUE;
      for (int i = 0; i < centers.length; i++) {
        distance = squaredDistance(currentFeature, centers[i]);
        if (distance < closestCenterDistance) {
          closestCenterDistance = distance;
          closestCenterIndex = i;
        }
      }
      /*
       * Update the WSSSE accumulator for this record.
       */
      costAccumulator.add(closestCenterDistance);
      clusterSums[closestCenterIndex] = addVectors(currentFeature, clusterSums[closestCenterIndex]);
      clusterCounts[closestCenterIndex] += 1;
    }

    List<Tuple2<Integer, Tuple2<double[], Long>>> preAveragedCenters =
        new ArrayList<>(centers.length);

    for (int i = 0; i < centers.length; i++) {
      /*
       * If clusterCount[i] = 0, then no records were found to be closest to this
       * center and thus the center will be dropped.
       */
      if (clusterCounts[i] > 0) {
        preAveragedCenters.add(new Tuple2<>(i, new Tuple2<>(clusterSums[i], clusterCounts[i])));
      }
    }
    return preAveragedCenters.iterator();
  }
  /**
   * Method for computing the squared distance between two vectors.
   *
   * @param a array
   * @param b array
   * @return squared distance
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

  /**
   * Method for performing vector addition. Assumes that the two arrays are of the same length.
   *
   * @param a array
   * @param b array
   * @return array of the summed arrays
   */
  private static double[] addVectors(double[] a, double[] b) {
    double[] sums = new double[a.length];
    for (int i = 0; i < a.length; i++) {
      sums[i] = a[i] + b[i];
    }
    return sums;
  }

  /**
   * Computes the centroid of already summed vectors against a given count.
   * @param sums vector of previously summed vectors
   * @param count number of vectors assigned to this centroid
   * @return new centroid array
   */
  private static double[] vectorAverage(double[] sums, long count) {
    double[] center = new double[sums.length];
    for (int i = 0; i < sums.length; i++) {
      center[i] = sums[i] / count;
    }
    return center;
  }
}
