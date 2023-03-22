import java.io.Serializable;
import java.util.Arrays;

/** Record representing a CSV row entry. */
public class Record implements Serializable {

  private static final long serialVersionUID = 1556420882164543759L;

  private long id;
  private double aggregateValue;
  private double[] feature;
  private String history;

  public Record(long id, double aggregateValue, double[] feature, String history) {
    this.id = id;
    this.aggregateValue = aggregateValue;
    this.feature = feature;
    this.history = history;
  }

  /**
   * Factory method for creating Record objects from an appropriate csv row and dimension.
   *
   * @param csvRow String CSV row entry.
   * @param dimension Dimension of the feature vector.
   * @return Record object with default partition and history.
   */
  public static Record fromCSVAndDim(String csvRow, int dimension) {
    String[] csvEntries = csvRow.split(",");
    double[] featureValues = new double[dimension];
    for (int i = 0; i < dimension; i++) {
      featureValues[i] = Double.parseDouble(csvEntries[i + 2].trim());
    }
    return new Record(
        Long.parseLong(csvEntries[0].trim()),
        Double.parseDouble(csvEntries[1].trim()),
        featureValues,
        "-1_-1");
  }

  /**
   * Factory method for creating Record objects with updated partition and history.
   *
   * @param assigned Index of the partition this record is assigned to.
   * @param base Index of the base partition this record belongs to.
   * @param record Record entry that will be updated.
   * @return Record with appended history and updated partition value.
   */
  public static Record updateHistory(int assigned, int base, Record record) {
    String updatedHist = assigned + "_" + base + "," + record.getHistory();
    return new Record(record.getId(), record.getAggregateValue(), record.getFeature(), updatedHist);
  }

  public long getId() {
    return id;
  }

  public double getAggregateValue() {
    return aggregateValue;
  }

  public double[] getFeature() {
    return feature;
  }

  public String getHistory() {
    return history;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(feature);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Record other = (Record) obj;
    if (!Arrays.equals(feature, other.feature)) {
      return false;
    }
    return true;
  }
  
  @Override
  public String toString() {
    String recordString = id + "," + aggregateValue + ",";
    for (int i = 0; i < feature.length; i++) {
      recordString += feature[i] + ",";
    }
    return recordString + history;
  }
}
