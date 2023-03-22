import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.Writable;

public class KMeansVector implements Writable {

  private long count;
  private double cost;
  private double[] feature;

  /*
   * This constructor is required to be Java Bean compliant and is
   * used by Hadoop for reflection when performing serialization.
   */
  public KMeansVector() {}

  public KMeansVector(long count, double cost, double[] vector) {
    this.count = count;
    this.cost = cost;
    this.feature = vector;
  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }

  public double getCost() {
    return cost;
  }

  public void setCost(double cost) {
    this.cost = cost;
  }

  public double[] getFeature() {
    return feature;
  }

  public void setFeature(double[] vector) {
    this.feature = vector;
  }

  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(feature.length);
    dataOutput.writeLong(count);
    dataOutput.writeDouble(cost);
    for (int i = 0; i < feature.length; i++) {
      dataOutput.writeDouble(feature[i]);
    }
  }

  public void readFields(DataInput dataInput) throws IOException {
    int dim = dataInput.readInt();
    count = dataInput.readLong();
    cost = dataInput.readDouble();
    feature = new double[dim];
    for (int i = 0; i < dim; i++) {
      feature[i] = dataInput.readDouble();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    KMeansVector that = (KMeansVector) o;

    if (count != that.count) {
      return false;
    }
    if (Double.compare(that.cost, cost) != 0) {
      return false;
    }
    return Arrays.equals(feature, that.feature);
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    result = (int) (count ^ (count >>> 32));
    temp = Double.doubleToLongBits(cost);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    result = 31 * result + Arrays.hashCode(feature);
    return result;
  }

  @Override
  public String toString() {
    StringBuilder vectorBuilder = new StringBuilder(this.feature.length + (this.feature.length - 1));
    for (int i = 0; i < this.feature.length; i++) {
      if (i < this.feature.length - 1) {
        vectorBuilder.append(feature[i]).append(",");
      } else {
        vectorBuilder.append(feature[i]);
      }
    }
    return feature.toString();
  }
}
