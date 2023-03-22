import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/*
 * This class is named GroupByVector as
 * it is used by GroupBy. Contains no special
 * functionality beyond a regular vector.
 */
public class GroupByVector implements Writable, WritableComparable<GroupByVector> {

  private double[] feature;

  public GroupByVector() {}

  public GroupByVector(double[] feature) {
    this.feature = feature;
  }

  public double[] getFeature() {
    return feature;
  }

  public void setFeature(double[] feature) {
    this.feature = feature;
  }

  public int compareTo(GroupByVector other) {
    for (int i = 0; i < feature.length; i++) {
      if (feature[i] != other.getFeature()[i]) {
        if (feature[i] < other.getFeature()[i]) {
          return -1;
        } else {
          return 1;
        }
      }
    }
    return 0;
  }

  public void readFields(DataInput in) throws IOException {
    int dimension = in.readInt();
    feature = new double[dimension];
    for (int i = 0; i < dimension; i++) {
      feature[i] = in.readDouble();
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(feature.length);
    for (int i = 0; i < feature.length; i++) {
      out.writeDouble(feature[i]);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(feature);
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
    GroupByVector other = (GroupByVector) object;
    if (!Arrays.equals(feature, other.getFeature())) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder featureBuilder = new StringBuilder(feature.length + (feature.length - 1));
    for (int i = 0; i < feature.length; i++) {
      if (i < feature.length - 1) {
        featureBuilder.append(String.valueOf(feature[i])).append(",");
      } else {
        featureBuilder.append(String.valueOf(feature[i]));
      }
    }
    return featureBuilder.toString();
  }
}
