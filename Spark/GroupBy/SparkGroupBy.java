import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

/** Perform classic grouping and aggregation operations. */
public final class SparkGroupBy {

  public static void main(String[] args) throws IOException {

    long t0 = System.nanoTime();
    
    /*
     * Spark initialization.
     */
    SparkConf conf = new SparkConf().setAppName("SparkGroupBy");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    jsc.setLogLevel("WARN");

    /*
     * Connection to HDFS.
     */
    Configuration hadoopConf = jsc.hadoopConfiguration();
    FileSystem hdfs = FileSystem.get(hadoopConf);

    /*
     * dimension Dimension of the feature vector. splits Number of underlying Spark
     * partitions. aggreeChoice Desired type of aggregation. 1: Average 2: Count 3:
     * Maximum 4: Minimum 5: Summation pathToRead Path to input CSV. pathToWrite
     * Path to output directory.
     */
    int splits = Integer.parseInt(args[0]);
    int dimension = Integer.parseInt(args[1]);
    int aggregationChoice = Integer.parseInt(args[2]);
    String pathToRead = args[3];
    String pathToWrite = args[4];

    /*
     * Appending name of cluster HDFS to path names.
     * Data must reside on cluster HDFS.
     *
     * Note: Might need to add an forward slash between HDFS name and path name.
     */
    //    pathToRead = hdfs.getUri() + "/" + pathToRead;
    //    pathToWrite = hdfs.getUri() + "/" + pathToWrite;

    String parameters =
        "Dimension: "
            + dimension
            + "\nSplits: "
            + splits
            + "\nAggregation Choice: "
            + aggregationChoice
            + "\nInput Path: "
            + pathToRead
            + "\nOutput Path: "
            + pathToWrite;

    System.out.println(parameters);
    /*
     * Read in CSV file. CSV file should be in the format: ID, Aggregation Value,
     * Vector Elements: X_1, X_2, ..., X_n
     */
    JavaRDD<String> csvStrRDD = jsc.textFile(pathToRead, splits);
    /*
     * Maps each line/record from csv file into key value-pairs. The record ID value
     * is not retained.
     *
     * Key: Vector, Value: Aggregation value
     */
    JavaPairRDD<Vector, Double> recordTupleRDD =
        csvStrRDD
            .mapToPair(
                csvStr -> {
                  String[] sarray = csvStr.split(",");
                  double[] vectorElements = new double[dimension];
                  double aggregationValue = Double.parseDouble(sarray[1]);
                  for (int i = 0; i < dimension; i++) {
                    vectorElements[i] = Double.parseDouble(sarray[i + 2]);
                  }
                  return new Tuple2<>(Vectors.dense(vectorElements), aggregationValue);
                })
            .partitionBy(new HashPartitioner(splits));
    /*
     * Compute the desired aggregation.
     */
    JavaPairRDD<Vector, Double> resultsRDD;
    String choice;
    switch (aggregationChoice) {
      case 1: // average
        resultsRDD =
            recordTupleRDD
                .aggregateByKey(
                    new Tuple2<>(0.0, 0.0),
                    (i, j) -> new Tuple2<>(i._1 + j, i._2 + 1),
                    (m, n) -> new Tuple2<>(m._1 + n._1, m._2 + n._2))
                .mapValues(v -> v._1 / v._2)
                .cache();
        choice = "average";
        break;
      case 2: // count
        resultsRDD = recordTupleRDD.mapValues(v -> 1.0).reduceByKey((a, b) -> a + b).cache();
        choice = "count";
        break;
      case 3: // max
        resultsRDD = recordTupleRDD.reduceByKey((a, b) -> a < b ? b : a).cache();
        choice = "max";
        break;
      case 4: // min
        resultsRDD = recordTupleRDD.reduceByKey((a, b) -> a < b ? a : b).cache();
        choice = "min";
        break;
      case 5: // sum
        resultsRDD = recordTupleRDD.reduceByKey((a, b) -> a + b).cache();
        choice = "sum";
        break;
      default: // average
        resultsRDD =
            recordTupleRDD
                .aggregateByKey(
                    new Tuple2<>(0.0, 0.0),
                    (i, j) -> new Tuple2<>(i._1 + j, i._2 + 1),
                    (m, n) -> new Tuple2<>(m._1 + n._1, m._2 + n._2))
                .mapValues(v -> v._1 / v._2)
                .cache();
        choice = "average";
    }

    /*
     * Standard time format HH:mm:ss results in path issues. Had to switch to HH-mm-ss format to avoid errors.
     */
    String date =
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")).toString();
    /*
     * The string fileName is actually used as a directory name.
     */
    String fileName = "SparkGroupBy_" + date + "_" + choice;
    resultsRDD.saveAsTextFile(pathToWrite + "/" + fileName);

    long numberGroups = resultsRDD.count();
    resultsRDD.unpersist();

    long t1 = System.nanoTime();

    String executionTimeStr = "Execution time (ns): " + (t1 - t0);
    String numberGroupsStr = "Groups Identified: " + numberGroups;

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

    /*
     * Delete output for another performance evaluation.
     */
    //    hdfs.delete(new Path(pathToWrite), true);
    jsc.close();
  }
}
