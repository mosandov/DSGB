import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.apache.hadoop.io.WritableComparable;

/** @author jcstarks */
public class VectorElem implements WritableComparable<VectorElem> {
  private static final Logger log = LogManager.getLogger(VectorElem.class);

  // public static int MaxPar = 5;// add by rct 2015-12-30

  // DATA PROPERTIES
  long key; // Vector key (ID)
  // public int dim; // Vector dimensionality
  public double aggVal = -1.0; // Record aggregation value ** FUTURE USE
  ArrayList<Double> elements; // Vector elements (dimensions) array
  public long assignedPartition = -1;
  public long basePartition = -1;
  public String parHistory = "";
  // if go through more than two rounds,
  // the history value comes from each round will be seperated by |
  // each history is like assigned1_base1|assigned2_base2|assigned3_base3

  // CONSTRUCTOR
  VectorElem() {
    elements = new ArrayList<>(); // Jeremy_Changed to be initialized -- avoids NullPointerException
  }

  VectorElem(int newDim) {
    // dim = newDim;
    elements = new ArrayList<>(newDim);
  }

  VectorElem(String strLine, int newDim) {
    log.setLevel(Level.OFF);

    elements = new ArrayList<>();
    //    System.out.println("Printing the value of the this constructor: " + strLine);

    StringTokenizer line = new StringTokenizer(strLine.trim(), ","); // Tokenize input string
    //    System.out.println(
    //        "dim: " + newDim + " Split line: " + Arrays.toString(strLine.trim().split(",")));
    //
    if (line.hasMoreElements()) {
      this.key = Long.parseLong(line.nextToken()); // Assign vector key
    }
    if (line.hasMoreElements()) {
      this.aggVal =
          Double.parseDouble(line.nextToken()); // Assign record aggregation value ** FUTURE USE
    }
    for (int i = 0; i < newDim; i++) {
      if (line.hasMoreElements()) {
        elements.add(Double.parseDouble(line.nextToken())); // adds element to the end of the list
      }
    }
    if (line.hasMoreElements()) { // NextRound uses from file
      this.assignedPartition = Long.parseLong(line.nextToken());
      log.debug("  ~assignedPart: " + assignedPartition);
    }
    if (line.hasMoreElements()) {
      this.basePartition = Long.parseLong(line.nextToken());
      log.debug("  ~basePart: " + basePartition);
    }
  } // end constructor

  VectorElem(String text, int newDim, boolean FurtherRound) {
    elements = new ArrayList<>();
    //    System.out.println("Further Round Contructor:  dim = " + newDim + " " + text);

    if (FurtherRound) {
      StringTokenizer line = new StringTokenizer(text.trim(), ",");
      if (line.hasMoreElements()) {
        this.key = Long.parseLong(line.nextToken()); // Assign vector key
      }
      if (line.hasMoreTokens()) {
        this.aggVal =
            Double.parseDouble(line.nextToken()); // Progress token to bypass aggregation value
      }
      for (int i = 0; i < newDim; i++) {
        if (line.hasMoreElements()) {
          elements.add(Double.parseDouble(line.nextToken())); // adds @ end of the list
        }
      }
      if (line.hasMoreElements()) {
        this.parHistory = line.nextToken();
      }
    }
  } // end contructor

  VectorElem(long key, ArrayList<Double> elems) {
    // VectorElem(long key, ArrayList<Double> elems, double aggVal) {
    this.key = key;
    // dim = elems.size();
    elements = new ArrayList<>();
    for (double element : elems) {
      elements.add(element); // adds at the end of the list
    }
  }

  VectorElem(VectorElem o) {
    this.key = o.key;
    // this.dim = o.dim;
    this.aggVal = o.aggVal;
    elements = new ArrayList<>();

    for (double element : o.elements) {
      elements.add(element); // adds at the end of the list
    }
    this.assignedPartition = o.assignedPartition;
    this.basePartition = o.basePartition;
    this.parHistory = o.parHistory; // Added 6.13.18_Jeremy
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    log.setLevel(Level.OFF);
    elements = new ArrayList<>();

    this.key = in.readLong(); // Read in the vector key
    // this.dim = in.readInt(); // Read in the vector dimensionality
    this.aggVal = in.readDouble(); // Read in the vector's aggregation value ** FUTURE USE
    int dim = in.readInt();

    for (int i = 0; i < dim; i++) {
      elements.add(
          in.readDouble()); // Jeremy_Changed from elements.set -- avoids IndexArrayOutOfBounds
    }
    this.assignedPartition = in.readLong();
    this.basePartition = in.readLong();
    this.parHistory = in.readUTF();
  } // end readFields method

  @Override
  public void write(DataOutput out) throws IOException {
    log.setLevel(Level.OFF);

    out.writeLong(key); // Write out the vector key
    // out.writeInt(dim); // Write out the vector dimensionality
    out.writeDouble(aggVal); // Write out the vector's aggregation value ** FUTURE USE
    out.writeInt(elements.size());

    for (double element : elements) {
      out.writeDouble(element);
    }
    out.writeLong(assignedPartition);
    out.writeLong(basePartition);
    out.writeUTF(parHistory);
  } // end write method

  @Override
  public int compareTo(VectorElem o) { // standard compare uses all the attributes
    int index = 0;
    for (double element : elements) { // compare vectors
      if (elements.get(index) != o.getElem(index)) {
        return (elements.get(index) < o.getElem(index)) ? -1 : 1;
      }
      index++;
    }

    return 0; // If all dims are equal

    /*if (index == dim) { // compare aggVals ** FUTURE USE
        return (this.aggVal < o.aggVal) ? -1 : 1;
    } else {
        return 0; // If all Dims AND aggVal are equal
    }*/
  } // end compareTo()

  @Override
  public int hashCode() {
    int prime = 31;
    int result = 0;

    for (double element : elements) {
      result += element * prime;
    }
    return result;
  } // end hashCode() //added by Jeremy 4.12.2018

  @Override
  public String toString() {
    String strRtn = key + "," + aggVal + ",";
    String elems = "";
    int index = 0;
    for (double element : elements) {
      elems += elements.get(index);
      index++;
      if (index < elements.size()) {
        elems += ",";
      }
    }
    strRtn = strRtn + elems;
    // return (key +","+ elems);
    return (strRtn);
  }

  public String toStringAggElems() {
    String elems = "";
    int index = 0;
    for (double element : elements) {
      elems += elements.get(index);
      index++;
      if (index != elements.size()) {
        elems += ",";
      }
    }
    return (aggVal + "," + elems);
  }

  public String toStringPart() { // For debug, display output
    String elems = "";
    int index = 0;
    for (double element : elements) {
      elems += elements.get(index);
      index++;
      if (index != elements.size()) {
        elems += ",";
      }
    }
    return (key + "," + aggVal + "," + elems + "," + assignedPartition + "_" + basePartition);
  }

  public String toStringAll() { // **For debug output -- not to be used for write output**
    String elems = "";
    int index = 0;
    for (double element : elements) {
      elems += elements.get(index);
      index++;
      if (index != elements.size()) {
        elems += ",";
      }
    }
    return (key
        + ","
        + aggVal
        + ","
        + elems
        + ","
        + assignedPartition
        + "_"
        + basePartition
        + ","
        + parHistory);
  }

  public double getDistanceBetween(VectorElem o) {
    double dist = 0.0;

    int index = 0;
    for (double element : elements) {
      dist += Math.pow((elements.get(index) - o.getElem(index)), 2);
      index++;
    }
    return Math.sqrt(dist);
  }

  public long getKey() {
    return key;
  }

  public void setKey(long newKey) {
    this.key = newKey;
  }

  public void setAggValue(double aggValue) { // FUTURE USE ** UNCOMMENT HERE TO USE
    this.aggVal = aggValue;
  }

  /*    public double getAggValue() {
          return aggVal;
  }       */
  // FUTURE USE ** UNCOMMENT HERE TO USE

  public double getElem(int index) {
    return elements.get(index);
  }

  public double setElem(int index, double elem) {
    return elements.set(index, elem);
  }

  public int getElemsSize() {
    // return dim;
    return elements.size();
  }

  public void setAssignedPartition(long assignedPartition) {
    this.assignedPartition = assignedPartition;
  }

  public long getAssignedPartition() {
    return assignedPartition;
  }

  public void setBasePartition(long basePartition) {
    this.basePartition = basePartition;
  }

  public long getBasePartition() {
    return basePartition;
  }

  public double getSizeInBytes() {
    return (Long.SIZE
            + // assignedPartition
            Long.SIZE
            + // basePartition
            Long.SIZE
            + // key
            Double.SIZE
            + // aggVal ** FUTURE USE
            (elements.size() * Double.SIZE) // all elements
        )
        / 8; // ** Should this also include parHistory ??
  }
} // end VectorElem class
