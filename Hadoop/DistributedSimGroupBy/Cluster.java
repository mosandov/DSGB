import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class Cluster {

  private static final Logger log = LogManager.getLogger(Cluster.class);

  // DATA
  private long clusterId = 0;
  private ArrayList<VectorElem> dataPoints = new ArrayList<>(); // list of vectors in this cluster
  private VectorElem centroid; // VectorElem object for centroid

  private static int dimSize = 0;
  // private static double aggVal = 0.0;

  // Array for holding partial sums of element values
  private ArrayList<Double> eSums = new ArrayList<>();
  private int clusterSize = 0;

  // CONSTRUCTOR
  Cluster() {
    log.setLevel(Level.OFF);
    log.debug("     ~Cluster() called:");

    // Generate zero value element values for centroid
    ArrayList<Double> centroidElems = new ArrayList<>();
    for (int i = 0; i < dimSize; i++) { // Can this actually see a non-zero dimSize?
      centroidElems.add(0.0);
    }

    // Create the new centroid as a VectorElem
    centroid = new VectorElem(clusterId, centroidElems);
    log.debug("       -centroid created: " + centroid.toStringPart());
    clusterId++;
  } // end constructor()

  Cluster(int vectSize) {
    log.setLevel(Level.OFF);
    log.debug("     ~Cluster(vectSize) called:");

    dimSize = vectSize;

    // Generate zero value element values for centroid
    ArrayList<Double> centroidElems = new ArrayList<>();
    for (int i = 0; i < dimSize; i++) {
      centroidElems.add(0.0);
    }

    for (int i = 0; i < dimSize; i++) {
      eSums.add(0.0);
    }
    // log.debug("Cluster.constructor(vSize)");
    // log.debug(" --eSums.size: " + eSums.size());

    // Create the new centroid as a VectorElem
    centroid = new VectorElem(clusterId, centroidElems);
    log.debug("       -centroid created: " + centroid.toStringPart());
    clusterId++;
    log.debug("     ~~Cluster(vectSize)complete.");
  } // end constructor(vectSize)

  // METHODS
  // get the number of elements in the cluster
  public int size() {
    return (this.dataPoints.size());
  }

  // get the cluster id number
  public long getClusterId() {
    return clusterId;
  }

  public void setClusterId(long newClusterId) {
    this.clusterId = newClusterId;
  }

  // add a VectorElem to the cluster
  public void addVector(VectorElem newElem) {
    log.setLevel(Level.OFF);
    log.debug("     ~Cluster.addVector(newElem) called:");
    log.debug("       -newElem: " + newElem);
    // dimSize = newElem.dim;
    dimSize = newElem.getElemsSize();
    dataPoints.add(newElem);
    log.debug("       -newElem added to dataPoints list.");

    // Add newElem values to elem partial sums
    // for (int i=0; i < newElem.dim; i++) {
    for (int i = 0; i < dimSize; i++) {
      double tmpSum = eSums.get(i) + newElem.getElem(i);
      eSums.set(i, tmpSum);
    }
    clusterSize++;
    log.debug("       -clusterSize: " + clusterSize);
    updateCentroid(); // new vector changes the centroid
    log.debug("       -updateCentroid() based on eSums.");
    log.debug("     ~~Cluster.addVector(newElem) complete.");
  } // end addVector

  // check if cluster is empty
  public boolean isEmpty() {
    return (this.dataPoints.isEmpty());
  }

  public ArrayList<VectorElem> getCluster() {
    return (this.dataPoints);
  }

  public VectorElem getVectorElem(int i) {
    return (this.dataPoints.get(i));
  }

  // get the current centroid value for the cluster
  public VectorElem getCentroid() {
    return centroid;
  }

  // update the centroid from a new list of VectorElem's
  public void updateCentroid() {
    for (int i = 0; i < dimSize; i++) {
      double elemSum = (eSums.get(i) / clusterSize);
      centroid.setElem(i, elemSum);
    }
  } // end updateCentroid method
} // end Cluster class
