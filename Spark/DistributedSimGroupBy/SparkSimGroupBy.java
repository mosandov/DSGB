import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public final class SparkSimGroupBy implements Serializable {

  private static final long serialVersionUID = -2678319486773694308L;

  private SparkSimGroupBy() {}

  public static void main(String[] args) throws IOException, InterruptedException {

    // Setting a timer for SimGroupBy.
    long t0 = System.nanoTime();

    SparkConf conf = new SparkConf().setAppName("SparkSimGroupBy");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    jsc.setLogLevel("WARN");

    /*
     * Connection to HDFS.
     */
    Configuration hadoopConf = jsc.hadoopConfiguration();
    FileSystem hdfs = FileSystem.get(hadoopConf);

    int splits = Integer.parseInt(args[0]);
    int dimension = Integer.parseInt(args[1]);
    int numberOfPivots = Integer.parseInt(args[2]);
    int threshold = Integer.parseInt(args[3]);
    long seed = Long.parseLong(args[4]);
    double epsilon = Double.parseDouble(args[5]);
    String pathToRead = args[6];
    String pathToWrite = args[7];

    /*
     * Appending name of cluster HDFS to path names.
     * Data must reside on cluster HDFS before running.
     */
    // pathToRead = hdfs.getUri() + "/" + pathToRead;
    // pathToWrite = hdfs.getUri() + "/" + pathToWrite;
    String parameters =
        "Splits: "
            + splits
            + "\nDimension: "
            + dimension
            + "\nNumber of Pivots: "
            + numberOfPivots
            + "\nThreshold: "
            + threshold
            + "\nEpsilon: "
            + epsilon
            + "\nSeed: "
            + seed
            + "\nInput Path: "
            + pathToRead
            + "\nOutput Path: "
            + pathToWrite;

    System.out.println(parameters);

    JavaRDD<String> csvStrings = jsc.textFile(pathToRead, splits);
    JavaRDD<Record> recordsRDD =
        csvStrings.map(csvRow -> Record.fromCSVAndDim(csvRow, dimension)).cache();

    long totalCount =
        simGroupBy(recordsRDD, threshold, splits, numberOfPivots, seed, epsilon, pathToWrite, jsc);

    // Ending timer.
    long t1 = System.nanoTime();

    String date =
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")).toString();

    String executionTimeStr = "Execution time (ns): " + (t1 - t0);
    String numberGroupsStr = "Groups Identified: " + totalCount;
    String fileName = "SparkSGB_" + date + ".txt";
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
            + numberGroupsStr
            + "\n"
            + executionTimeStr;

    System.out.println("Summary:" + summary);

    Path statsPath = new Path(hdfs.getUri() + "/stats/Stats_" + fileName);
    FSDataOutputStream statsFile = hdfs.create(statsPath);
    statsFile.writeUTF(summary);
    statsFile.close();

    // Delete output for next performance evaluation.
    // hdfs.delete(new Path(pathToWrite), true);

    jsc.close();
  }

  public static long simGroupBy(
      JavaRDD<Record> recordsRDD,
      int threshold,
      int splits,
      int numberOfPivots,
      long seed,
      double epsilon,
      String pathToWrite,
      JavaSparkContext jsc)
      throws InterruptedException {

    long count = 0;
    /*
     * Sample the records for the partitioning phase. Pivots are implicitly passed by Spark in the upcoming
     * map that is performed.
     *
     * Note: The sample() method is faster than takeSample as
     * takeSample() also performs a count operation.
     */
    final double[][] pivots =
        recordsRDD
            .takeSample(true, numberOfPivots, seed)
            .stream()
            .map(Record::getFeature)
            .toArray(double[][]::new);

    System.out.println("Number of Pivots: " + pivots.length);
    /*
     * Performing the partitioning and cache the results.
     * It's important operation to cache if the partitions contain more elements then the threshold.
     */
    JavaPairRDD<Integer, Record> partitionedSpaceRDD =
        recordsRDD
            .mapPartitionsToPair(
                recordsIterator -> SGBFunctions.partitionRecords(recordsIterator, pivots, epsilon))
            .partitionBy(new HashPartitioner(splits))
            .cache();
    /*
     * The unassigned records passed in the function parameter are released from memory.
     */
    recordsRDD.unpersist();
    /*
     * Counts the number of records assigned to each partition.
     */
    Map<Integer, Long> partitionCounts = partitionedSpaceRDD.countByKey();
    System.out.println("Number of Partitions: " + partitionCounts.size());

    /*
     * Take the counts associated with each partition and separate them into two maps based on
     * whether their record counts are above or below a given threshold. The first map,
     * partitionsToCluster, contains the indices and counts of which partitions qualify to have the
     * clustering phase performed over them. The second map, partitionsToRepartion, stores the index
     * of the partitions which did not qualify to have clustering performed.
     */
    Map<Integer, Long> partitionsToCluster =
        partitionCounts
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue() <= threshold)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    Map<Integer, Long> partitionsToRepartion =
        partitionCounts
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue() > threshold)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    if (partitionsToCluster.size() == partitionCounts.size()) {
      System.out.println(
          "All " + partitionsToCluster.size() + " partitions will have clustering performed.");

      /*
       * If the number of partitions that qualify to have clustering performed is equal to the
       * number of partitions, then the clustering is performed, the clusters are written to disk,
       * and SimGroupBy terminates.
       */
      JavaRDD<Cluster> clustersRDD =
          partitionedSpaceRDD
              .groupByKey()
              .flatMap(
                  keyGroupedRecordsTuple ->
                      SGBFunctions.clusterPartition(keyGroupedRecordsTuple._2.iterator(), epsilon))
              .cache();
      /*
       * Release partitionedSpaceRDD, necessary if this within a recursive call to SimGroupBy().
       */
      partitionedSpaceRDD.unpersist();

      /*
       * Output directory name is "SparkSGB_[time write job was called]",
       * his was chosen to avoid directory name collision.
       */
      String date =
          LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")).toString();
      String outputDirectoryName = "SparkSGB_" + date;
      clustersRDD.saveAsTextFile(pathToWrite + "/" + outputDirectoryName);

      count += clustersRDD.count();
      /*
       * No needed used. Release from memory.
       */
      clustersRDD.unpersist();
      return count;
    } else if (0 < partitionsToCluster.size()
        && partitionsToCluster.size() < partitionCounts.size()) {

      System.out.println(
          partitionsToRepartion.size()
              + " partitions need reparittioning, "
              + partitionsToCluster.size()
              + " partitions will have clustering performed.");

      /*
       * If the there exist a combination of partitions that satisfy the criteria
       * for clustering (below the threshold) and those that that don't, then
       * first: perform clustering only on those that do,
       * second: recursively send the remaining partitions through SimilarityGroupBy().
       */

      /*
       * Retrieve the indices associated with each partition and store them an array.
       * This step is done to allow the use of binarySearch() and reduce the size of
       * the object implicitly sent to the filter operation.
       */
      int[] partitionsToCluserIndices =
          partitionsToCluster.keySet().stream().mapToInt(Integer::intValue).toArray();
      /*
       * Preliminary sorting before use of binarySearch().
       */
      Arrays.sort(partitionsToCluserIndices);

      /*
       * Filter elements based on whether the index of the partition
       * they belong to qualifies for clustering.
       */
      JavaRDD<Cluster> clustersRDD =
          partitionedSpaceRDD
              .filter(
                  keyRecordTuple ->
                      Arrays.binarySearch(partitionsToCluserIndices, keyRecordTuple._1) >= 0)
              .groupByKey()
              .flatMap(
                  keyGroupedRecordsTuple ->
                      SGBFunctions.clusterPartition(keyGroupedRecordsTuple._2.iterator(), epsilon))
              .cache();

      String date =
          LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")).toString();

      /*
       * Output directory name is "SparkSGB_[time write job was called]",
       * his was chosen to avoid directory name collision.
       */
      String outputDirectoryName = "SparkSGB_" + date;
      clustersRDD.saveAsTextFile(pathToWrite + "/" + outputDirectoryName);

      count += clustersRDD.count();
      /*
       * No longer used. Release from memory.
       */
      clustersRDD.unpersist();
      /*
       * Filter elements based on whether the index of the partition
       * they belong to does not qualify for clustering.
       *
       * Resulting RDD is cached to speed up recursive calls.
       */
      JavaPairRDD<Integer, Record> repartitionPartitionsRDD =
          partitionedSpaceRDD
              .filter(
                  keyRecordTuple ->
                      Arrays.binarySearch(partitionsToCluserIndices, keyRecordTuple._1) < 0)
              .cache();
      /*
       * The partitionedSpaceRDD is no longer needed as it's already been
       * filtered into two seperate RDDs. Memory is released.
       */
      partitionedSpaceRDD.unpersist();

      for (Map.Entry<Integer, Long> pair : partitionsToRepartion.entrySet()) {
        /*
         * Filter RDD per partition, then recursively send each partition similarityGroupBy().
         * The number of pivots << number of partition elements as otherwise
         * an inefficient amount of replication will occur.
         */
        final int partitonIndex = pair.getKey();
        JavaRDD<Record> thisPartition =
            repartitionPartitionsRDD
                .filter(keyRecordTuple -> keyRecordTuple._1 == partitonIndex)
                .map(t -> t._2) // Grab only the record from this tuple pair.
                .cache();

        count +=
            simGroupBy(
                thisPartition, threshold, splits, numberOfPivots, seed, epsilon, pathToWrite, jsc);
      }
      return count;
    } else {
      System.out.println(
          "All " + partitionsToRepartion.size() + " partitions will go through a new iteration.");
      /*
       * If none of the partitions qualify for clustering (more elements then allowed by threshold),
       * recursively send each partition through similarityGroupBy().
       */
      for (Map.Entry<Integer, Long> pair : partitionsToRepartion.entrySet()) {
        /*
         * Filter RDD per partition. Send back through similarityGroupBy().
         * The number of pivots << number of partition elements as otherwise
         * an inefficient amount of replication will occur.
         */
        final int partitonIndex = pair.getKey();
        System.out.println(partitonIndex);
        JavaRDD<Record> thisPartition =
            partitionedSpaceRDD
                .filter(keyRecordTuple -> keyRecordTuple._1 == partitonIndex)
                .map(t -> t._2) // Grab only the record from this tuple pair.
                .cache();

        count +=
            simGroupBy(
                thisPartition, threshold, splits, numberOfPivots, seed, epsilon, pathToWrite, jsc);
      }
      /*
       * Release memory of partitionSpaceRDD. Important to release if this RDD exists
       * as the result of a recursive call.
       */
      partitionedSpaceRDD.unpersist();
      return count;
    }
  }
}
