import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import scala.Tuple2;

/** Collection of functions used by the SimGroupBy algorithm. */
public final class SGBFunctions implements Serializable {

  private static final long serialVersionUID = -6159033102743069405L;

  public SGBFunctions() {}

  /**
   * Partitions the space based on a random set of pivots. Records that are within epsilon to each
   * other are replicated in the appropriate partitions.
   *
   * @param recordsIterator Iterator containing the records of the given Spark partition.
   * @param pivots Randomly selected vectors used for partitioning the vector space.
   * @param epsilon Value used for defining the length of the extended window of each partition.
   * @param metric Metric used to define distance between records.
   * @return Iterator of updated records assigned to different partitions.
   */
  public static Iterator<Tuple2<Integer, Record>> partitionRecords(
      Iterator<Record> recordsIterator, double[][] pivots, double epsilon) {

    Record record;
    double closestPivotDistance;
    int closestPivotIndex;
    double distance;
    List<Tuple2<Integer, Record>> pairs = new ArrayList<>();

    while (recordsIterator.hasNext()) {
      record = recordsIterator.next();

      /*
       * Non-window partitioning.
       */
      closestPivotIndex = 0;
      closestPivotDistance = Double.MAX_VALUE;
      for (int i = 0; i < pivots.length; i++) {
        distance = metric(record.getFeature(), pivots[i]);
        if (distance < closestPivotDistance) {
          closestPivotDistance = distance;
          closestPivotIndex = i;
        }
      }
      pairs.add(
          new Tuple2<>(
              closestPivotIndex,
              Record.updateHistory(closestPivotIndex, closestPivotIndex, record)));
      /*
       * Window partitioning.
       *
       * ** For traditional Euclidean distance, the currently implemented distance to hyper-plane
       * formula is to be used. For a general metric space, the commented lower bond distance
       * formula is to be used. **
       */
      for (int i = 0; i < pivots.length; i++) {
        if (i != closestPivotIndex) {

          // distance = (euclideanMetric(record.getFeature(), randomPivots[i]) -
          // closestPivotDistance)
          // / 2;

          distance = metric(record.getFeature(), pivots[i]);
          distance =
              ((distance * distance) - (closestPivotDistance * closestPivotDistance))
                  / (2 * metric(pivots[i], pivots[closestPivotIndex]));
          if (distance <= epsilon) {
            pairs.add(new Tuple2<>(i, Record.updateHistory(i, closestPivotIndex, record)));
          }
        }
      }
    }
    return pairs.iterator();
  }

  /**
   * Clusters the given records based on a epsilon and a given metric.
   *
   * @param recordIterator Iterator of records for the given partition.
   * @param epsilon Value used for defining the length of the extended window of each partition.
   * @param metric Metric used to define distance between records.
   * @return Iterator of clusters. Can return an empty iterator if all of the clusters determine
   *     they should not be out given their partitioning history.
   */
  public static Iterator<Cluster> clusterPartition(
      Iterator<Record> recordIterator, double epsilon) {

    List<Cluster> clusters = new ArrayList<>();
    Record currentRecord;
    boolean clusterFound;
    boolean withinEpsilon;

    /*
     * Grab the first record in the partition and create the initial cluster.
     */
    clusters.add(new Cluster(recordIterator.next()));

    /*
     * Final clustering phase: For each remaining record in the partition, each record will either
     * be assigned to an existing cluster, or they become the center of new cluster to which all
     * future records will be compared against.
     */
    while (recordIterator.hasNext()) {
      currentRecord = recordIterator.next();
      clusterFound = false;
      withinEpsilon = false;

      /*
       * For each cluster, if a record is within epsilon to both the given cluster's centroid and
       * all other records within the cluster, then the record is assigned to the cluster.
       * Otherwise, a new cluster is created with this unassigned record as the initialization
       * record. Any new clusters will be added to the running list of existing clusters.
       */
      for (Cluster cluster : clusters) {
        if (metric(cluster.getCentroid(), currentRecord.getFeature()) <= epsilon) {
          withinEpsilon = true;
          for (Record clusterRecord : cluster.getRecords()) {
            if (metric(clusterRecord.getFeature(), currentRecord.getFeature()) > epsilon) {
              withinEpsilon = false;
              break;
            }
          }
          if (withinEpsilon) {
            cluster.addRecord(currentRecord);
            clusterFound = true;
            break;
          }
        }
      }
      if (!clusterFound) {
        clusters.add(new Cluster(currentRecord));
      }
    }

    /*
     * Removing the clusters that should not be output in this partition.
     */
    return clusters.stream().filter(cluster -> cluster.shouldClusterOutput()).iterator();
  }

  /**
   * Function for updating the pivots ratio for a another round if the current ratio would could
   * cause an infinite loop.
   *
   * @param partitionSize Size of a the given partition.
   * @param ratio Current ratio used for selecting the sample of random pivots.
   * @return New ratio. Will result in partitioning the next space/round by at least 3 pivots.
   */
  public static double ratioUpdater(long partitionSize, double ratio) {
    while (partitionSize * ratio <= 3) {
      ratio = ratio * 10;
    }
    return ratio;
  }

  /**
   * Calculate the Euclidean distance between two arrays/vectors. Does not depend on any external
   * libraries.
   *
   * @param a First array.
   * @param b Second array.
   * @return Euclidean distance between arrays.
   */
  public static double metric(double[] a, double[] b) {
    double squaredDistance = 0;
    double difference = 0;
    for (int i = 0; i < a.length; i++) {
      difference = a[i] - b[i];
      squaredDistance += difference * difference;
    }
    return Math.sqrt(squaredDistance);
  }
}
