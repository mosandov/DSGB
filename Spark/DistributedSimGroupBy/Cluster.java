import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Cluster for storing records. */
public final class Cluster implements Serializable {

  private static final long serialVersionUID = -3066790840699708700L;

  private List<Record> records = new ArrayList<>();
  private double[] preAverage;
  private double[] centroid;

  Cluster(Record record) {
    records.add(record);
    preAverage = Arrays.copyOf(record.getFeature(), record.getFeature().length);
    centroid = Arrays.copyOf(record.getFeature(), record.getFeature().length);
  }

  /**
   * Assigns a record to this cluster and updates the cluster centroid.
   *
   * @param record Record object being added to the cluster.
   */
  public void addRecord(Record record) {
    records.add(record);
    for (int i = 0; i < preAverage.length; i++) {
      preAverage[i] += record.getFeature()[i];
      centroid[i] = preAverage[i] / records.size();
    }
  }

  /**
   * Create the assigned partition index array from an arbitrary record.
   *
   * @param records List of records contained within the cluster.
   * @returns Universal assigned history array of the cluster.
   */
  private static int[] getRecordsAssignedHistory(List<Record> records) {
    String universalHistoryStr = records.get(0).getHistory();
    String[] historyArray =
        universalHistoryStr.substring(0, universalHistoryStr.length() - 6).split(",");
    return Arrays.stream(historyArray)
        .mapToInt(str -> Integer.parseInt(str.split("_")[0]))
        .toArray();
  }

  /**
   * Create the base history matrix of the cluster.
   *
   * @param records List of records contained within the cluster.
   * @return Base history matrix.
   */
  private static int[][] getRecordsBaseHistoryMatrix(List<Record> records) {
    /*
     * For each record, we grab the base partition index history string, convert it into a integer
     * array, and use the array as a row in the base history matrix.
     *
     * Properties of the matrix: Number of columns is equal to the number of rounds, number of rows
     * is equal to the number of records in this cluster.
     */
    String thisHistStr;
    String[] thisHistArray;
    int[][] baseHistoryMatrix = new int[records.size()][];

    for (int row = 0; row < baseHistoryMatrix.length; row++) {
      thisHistStr = records.get(row).getHistory();
      thisHistArray = thisHistStr.substring(0, thisHistStr.length() - 6).split(",");
      baseHistoryMatrix[row] =
          Arrays.stream(thisHistArray)
              .mapToInt(str -> Integer.parseInt(str.split("_")[1]))
              .toArray();
    }
    return baseHistoryMatrix;
  }

  /**
   * Determine whether a cluster should be output based on the base history matrix and assigned
   * history of this cluster.
   *
   * @return Boolean based on whether the assigned history of the records is equal to the minimum
   *     base history of each round.
   */
  public boolean shouldClusterOutput() {
    int[] recordsAssignedHistory = getRecordsAssignedHistory(this.records);
    int[][] recordsBaseHistoryMatrix = getRecordsBaseHistoryMatrix(this.records);

    int columnEntry;
    int currentColumnMinimum;
    for (int col = 0; col < recordsBaseHistoryMatrix[0].length; col++) {
      currentColumnMinimum = Integer.MAX_VALUE;
      for (int row = 0; row < recordsBaseHistoryMatrix.length; row++) {
        columnEntry = recordsBaseHistoryMatrix[row][col];
        if (columnEntry < currentColumnMinimum) {
          currentColumnMinimum = columnEntry;
        }
      }
      if (recordsAssignedHistory[col] != currentColumnMinimum) {
        return false;
      }
    }
    return true;
  }

  public List<Record> getRecords() {
    return records;
  }

  public double[] getCentroid() {
    return centroid;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(centroid);
    return result;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null) {
      return false;
    }
    if (getClass() != object.getClass()) {
      return false;
    }
    Cluster other = (Cluster) object;
    if (!Arrays.equals(centroid, other.centroid)) {
      return false;
    }
    return true;
  }

  /**
   * The toString() method controls how the cluster will be written as text data by Spark. An
   * appropriate output format should be determined.
   *
   * <p>Note: Currently only writing center. Writing all the records can be a huge amount of data to
   * write.
   */
  @Override
  public String toString() {
    return Arrays.toString(centroid);
  }
}
