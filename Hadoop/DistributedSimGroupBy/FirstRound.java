import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class FirstRound {

  public static class FirstMap extends Mapper<LongWritable, Text, LongWritable, VectorElem> {

    private static final Logger log = LogManager.getLogger(FirstRound.class);
    private static ArrayList<VectorElem> pivotPoints; // ArrayList to hold pivot points
    private static String pivotsPath;
    private static double epsilon; // Value of epsilon
    private static int numOfPivots; // Number of pivot pts. to select
    private static int vectorSize; // Dimensionality of vectors

    protected void setup(Context context) throws IOException {
      log.setLevel(Level.OFF);
      log.debug("Starting FirstRound.FirstMap.setup:");
      Configuration conf = context.getConfiguration();
      pivotsPath = conf.get("currentPivotsPath", "THIS_WENT_WRONG");
      vectorSize = conf.getInt("vectorSize", 0);
      epsilon = conf.getDouble("eps", 0);
      pivotPoints = new ArrayList<>();
      URI[] files = context.getCacheFiles();

      /*
       * I can't remember the motivation behind this.
       */
      for (int i = 0; i < files.length; i++) {
        System.out.println("piv path = " + files[i].getPath());
      }

      System.out.println("First Map Pivot Path = " + pivotsPath);
      System.out.println("Eps Firs Map setup = " + epsilon);
      System.out.println("Vector size First Map setup = " + vectorSize);
      BufferedReader pivotsBuffer = new BufferedReader(new FileReader(pivotsPath));

      pivotsBuffer.lines().forEach(line -> pivotPoints.add(new VectorElem(line, vectorSize)));
      pivotsBuffer.close();

      numOfPivots = pivotPoints.size();
      System.out.println(" --Pivot Points first round map set up: " + numOfPivots);
      log.debug("FirstRound.FirstMap.setup complete.");
    } // end setup

    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context)
        throws IOException, InterruptedException {

      log.setLevel(Level.OFF);
      // System.out.println("DBG: Entering FirstMap-map | FirstRound/FirstMap/map");
      log.debug("Starting FirstRound.FirstMap.map");
      // Configuration conf = context.getConfiguration();

      // pivPts and eps have been establisheds in the setup
      // create the element that is being worked on in this map
      //      System.out.println("Mapper value: " + inputValue);
      VectorElem currentVector = new VectorElem(inputValue.toString(), vectorSize);
      // log.debug(" --vec: " + vec.toString());
      VectorElem closestPivot = null; // current best pivot point/partition

      // current min distance between elem and closestPivot

      //      System.out.println("Is pivPts null for some reason?");
      //      pivPts.stream().forEach(System.out::println);
      double minimumDistance = Double.MAX_VALUE;
      for (VectorElem currentPivot2 : pivotPoints) {
        double currentDist = currentVector.getDistanceBetween(currentPivot2);
        if (currentDist < minimumDistance) {
          closestPivot = currentPivot2; // update closestPivot
          minimumDistance = currentDist; // update current distance
        }
      }

      // set element assigned partition
      currentVector.setAssignedPartition(closestPivot.getKey());
      // set element base/ partition
      currentVector.setBasePartition(closestPivot.getKey());
      // Output <closestPivot, {id, vec, closestPivot, closestPivot}>;
      context.write(new LongWritable(closestPivot.getKey()), currentVector);
      //      log.debug(
      //          " --context.write(CLOSESTPivot,vec): " + closestPivot.getKey() + "," +
      // currentVector);
      // System.out.println("    My vector: " + vec.toStringPart());
      // System.out.println("    My closest pivot: " + closestPivot.getKey());
      // log.debug(" --This vector: " + vec.toStringPart());
      // log.debug(" --My CLOSEST pivot: " + closestPivot.getKey());

      // there are repeat distance computation
      log.debug(" --Checking for distance repeats...");
      for (VectorElem currentPivot : pivotPoints) {
        // Now consider duplicates
        // get a current working pivot
        // currentPivot is the same as the best match
        if (currentPivot.getKey() == closestPivot.getKey()) {
          continue;
        }

        /*
         * The following works for generic data: d'(x ,P0
         * ,P1)=(dist(x,P1)-dist(x,P0))/2
         *
         * The following would have better performance but only works on
         * Euclidean Data:
         * d��=d=(dist(x,P1)^2-dist(x,P0)^2)/(2*dist(P0,P1))
         */

        // get the distance between x and P0
        double xP0 = currentVector.getDistanceBetween(closestPivot);
        // get the distance between x and P1
        double xP1 = currentVector.getDistanceBetween(currentPivot);
        // get the distance between P0 and P1

        double P0P1 = closestPivot.getDistanceBetween(currentPivot);
        // if(((xP1-xP0)/2) <= eps) {
        // generic expression -see formulas noted above.
        // window is on base partition closestPivot
        if (((Math.pow(xP1, 2)) - (Math.pow(xP0, 2))) / (2 * P0P1) <= epsilon) {
          // vec belongs to eps-window between currentPivot and
          // closestPivot
          currentVector.setAssignedPartition(currentPivot.getKey());
          currentVector.setBasePartition(closestPivot.getKey());
          // Output<P, {id, vec, P, closestPivot}>;
          context.write(new LongWritable(currentPivot.getKey()), currentVector);
          //          log.debug(
          //              " --context.write(CURRENTPivot,vec): " + currentPivot.getKey() + "," +
          // currentVector);
          // log.debug(" --This vector: " + vec.toStringPart());
          // log.debug(" --My CURRENT pivot: " + currentPivot.getKey());
        }
      } // end while(pivItr.hasNext()
      log.debug("FirstRound.FirstMap.map complete.");
    } // end map
  } // end FirstMap

  public static class FirstReduce extends Reducer<LongWritable, VectorElem, Text, Text> {

    private static final Logger log = LogManager.getLogger(FirstRound.class);
    private static ArrayList<VectorElem> pivotPoints; // ArrayList to hold pivot points
    private static String pivotsPath; // Symbolic link to job specific pivots.
    private static double epsilon; // Value of epsilon
    private static int numOfPivots; // Number of pivot pts. to select
    private static int vectorDimension; // Dimensionality of vectors
    private static int iteration;
    private static int partitionUpperBound;
    private static String inputPath;
    private static String numbersPath;

    @Override
    protected void setup(Context context) throws IOException {
      log.setLevel(Level.OFF);
      log.debug("Starting FirstRound.FirstReduce.setup:");

      Configuration conf = context.getConfiguration();
      inputPath = conf.get("InputDir");
      numbersPath = conf.get("numbersInParDir");
      pivotsPath = conf.get("currentPivotsPath", "THIS_WENT_WRONG");
      epsilon = conf.getDouble("eps", 0);
      vectorDimension = conf.getInt("vectorSize", 0);
      partitionUpperBound = conf.getInt("PartitionUpBound", 0);
      iteration = conf.getInt("Iteration", 0);
      pivotPoints = new ArrayList<>();

      System.out.println("pivs path: " + pivotsPath);
      System.out.println("Vector size Reduce setup " + vectorDimension);
      BufferedReader pivotsBuffer = new BufferedReader(new FileReader(pivotsPath));
      pivotsBuffer.lines().forEach(line -> pivotPoints.add(new VectorElem(line, vectorDimension)));
      pivotsBuffer.close();

      numOfPivots = pivotPoints.size();
      System.out.println("Pivot Points First Round Reduce set up: " + numOfPivots);
      log.debug("FirstRound.FirstReduce.setup complete.");
    } // end setup

    @Override
    public void reduce(LongWritable key, Iterable<VectorElem> values, Context context)
        throws IOException, InterruptedException {
      log.setLevel(Level.OFF);
      log.debug("Starting FirstRound.FirstReduce.reduce");
      //      System.out.println("Pivot Points First Round Reducer:" + pivotPoints.size());

      Configuration conf = context.getConfiguration();
      // eps is established in the reduce Setup

      log.debug(" --key (CurrentPivot ID): " + key);

      // create a list to hold the records
      ArrayList<VectorElem> recordsList = new ArrayList<>();
      int recordCount = 0;
      // log.debug(" --records.size(): " + records.size());

      // iterate values from map
      Iterator<VectorElem> valuesIterator = values.iterator();
      while (valuesIterator.hasNext()) {

        // pointer element
        VectorElem pointerElement = valuesIterator.next();

        // current element
        VectorElem currentElement = new VectorElem(pointerElement);
        //        log.debug(
        //            " --RecordCount: " + recordCount + " | currElem: " +
        // currentElement.toStringPart());
        // log.debug(" --currElem: " + currElem.toStringAll());

        // add current element to records list
        recordsList.add(currentElement);
        // DBG: echo VectorElems created from map output
        // log.debug("  --currElem: " + currElem.toStringPart());
        // log.debug("  -- currElem(records): " + records.get(RdCount));
        recordCount++;
      }

      log.info("Record Count:" + recordCount + " | PUB :" + partitionUpperBound);
      // System.out.println("Record Count:" + RdCount + "; PUB :" + PUB );

      if (recordCount > partitionUpperBound) {
        log.debug(" --RdCount > PUB: " + recordCount + "|" + partitionUpperBound);
        log.info("*****output for next round****");
        // System.out.println("*****output for next round****");
        // write our for next round job
        // write out the number of points in the partition
        // add by rct on 20160122
        Path outNumber =
            new Path(
                numbersPath + String.valueOf(iteration + 1) + "/" + key + "/" + key.toString());
        FileSystem fsTmp = FileSystem.get(conf);
        FSDataOutputStream outTmp = fsTmp.create(outNumber);
        outTmp.writeBytes(String.valueOf(recordCount));
        outTmp.close();
        // write the large partition to hdfs file
        // output file path is: someInputDir/String.valueof(Iter+1)/filename
        // one output partition in one folder  key/key
        Path outPath =
            new Path(inputPath + String.valueOf(iteration + 1) + "/" + key + "/" + key.toString());
        FileSystem fs = FileSystem.get(conf);
        // write the large partition to hdfs file
        FSDataOutputStream out = fs.create(outPath);
        log.debug(" --Next iteration output file created: " + outPath);
        // Write to HDFS file
        for (VectorElem vector : recordsList) {
          out.writeBytes(
              vector.key
                  + ","
                  + vector.toStringAggElems()
                  + ","
                  + vector.parHistory
                  /*+"|"*/ + vector.assignedPartition
                  + "_"
                  + vector.basePartition
                  + "\n");
        }
        out.close();
      } else { // Modify to happen when conditions met OR #ofPivots was 1 (maybe/maybe not necessary
        // here)
        log.setLevel(Level.OFF);
        log.debug(" --RdCount !> PUB: " + recordCount + "|" + partitionUpperBound);
        log.info("*****Output the cluster*****");

        // create a list of clusters
        ArrayList<Cluster> clusters = createClusters(epsilon, recordsList, vectorDimension);
        log.debug("  --createClusters(eps,recs): " + epsilon + ", " + recordsList.toString());

        // for each cluster C in clusters
        for (int i = 0; i < clusters.size(); i++) {
          Cluster cluster = clusters.get(i);

          // compute the flag array for the cluster
          int[] flagArray = computeFlagArray(cluster, numOfPivots);

          // flag for each pivot; 1 = there is one record for base partition
          long minimumFlag = computeMinFlag(flagArray);
          if (minimumFlag == key.get()) {
            context.getCounter("Groups_Identified", "counter").increment(1L);
            // Output cluster
            Text outKey =
                new Text("========== \t" + cluster.getClusterId()); // Create for separator
            Text outElem = new Text("=========="); // Create for separator

            context.write(outKey, outElem);

            log.debug(" --context.write(outKey,outElem): " + outKey + "," + outElem);
            // Output each element of the cluster
            for (int j = 0; j < cluster.size(); j++) {
              VectorElem element = cluster.getVectorElem(j);
              outKey = new Text("" + element.key); // Set vector key as output key
              // outElem = new Text("" + element.toStringElems()); // Set vector elements as output
              // value
              outElem =
                  new Text("" + element.toStringAggElems()); // Set vector elements as output value

              context.write(outKey, outElem); // Output key & value to files
            }
          } // end for output each element of the cluster
        } // end if output cluster
      } // end for each cluster C in clusters
    } // end else
  } // end reduce
  // modified by rct 2015-12-18

  private static ArrayList<Cluster> createClusters(
      double eps, ArrayList<VectorElem> points, int vectorSize) {

    // create a list to hold the clusters
    ArrayList<Cluster> clusterList = new ArrayList<>();

    // for each point p in S
    for (int i = 0; i < points.size(); i++) {
      VectorElem element = points.get(i);

      // set flag - each data point needs to be in a cluster
      boolean clusterFound = false;
      // set flag - initially assume points are not withinEps
      boolean withinEpsilon = false;

      // for each cluster Ci in C
      for (int j = 0; j < clusterList.size(); j++) {
        if (clusterList.isEmpty()) {
          // System.out.println(elem + "No clusters yet");
          break;
        }

        Cluster currentCluster = clusterList.get(j);
        // filter valid for vector data
        if (element.getDistanceBetween(currentCluster.getCentroid()) > eps) {
          // p does not belong in this cluster
          // System.out.println(elem + " Distance from pivot is more than epsilon");
        } else {
          // p may or may not belong to Ci
          // p is within eps of Ci centroid.
          withinEpsilon = true;

          // Now verify all points in C1 are within eps of p
          // for each VectorElem x in Ci
          for (int k = 0; k < currentCluster.size(); k++) {
            // holds current VectorElem x in Ci
            VectorElem clusterElem = currentCluster.getVectorElem(k);
            // p does not belong to Ci
            if (element.getDistanceBetween(clusterElem) > eps) {
              // System.out.println(elem + " Distance from other points is more than epsilon");
              withinEpsilon = false;
              break;
            } // end if p does not belong to Ci
          } // end for each VectorElem x in Ci

          if (withinEpsilon) {
            // p belongs to Ci
            currentCluster.addVector(element);
            // currentCluster.updateCentroid();
            // this could be done in the cluster
            clusterFound = true;
            // System.out.println(elem + " I was added to a cluster");
            break;
          } // end if p belongs to Ci
        } // end else p may or may not belong to Ci
      } // end for each cluster Ci in C

      if (!clusterFound) {
        // we did not find a cluster for p
        // then p must belong to a new cluster
        Cluster newCluster = new Cluster(vectorSize);
        newCluster.addVector(element);
        clusterList.add(newCluster); // add new cluster to cluster list
        // System.out.println(elem + " New cluster created");
      } // end if we did not find a cluster for p
    } // end for each point p in S

    return clusterList;
  } // end createClusters
  /*
   * computeFlagArray (Cluster) Int flagArray= new int[numOfPivots] //the
   * number of numOfPivots will be known to all the reducers For each
   * record in Cluster int recordPartition=record.getBasePartition();
   * //Grab the Base Partition to which this record belongs and assign the
   * corresponding array position to 1 flagArray[recordPartition]=1; If
   * all elements of flagArray are 1 break;
   */
  private static int[] computeFlagArray(Cluster cluster, int numOfPivots) {
    // the number of numOfPivots will be know to all the reducers
    int[] flagArray = new int[numOfPivots];
    // for each record in cluster
    for (int i = 0; i < cluster.size(); i++) {
      // Grab the basePartion for this record
      VectorElem record = cluster.getVectorElem(i);
      long recordPartition = record.getBasePartition();
      // assign the corresponding array position to 1
      flagArray[(int) recordPartition] = 1;
      // holds the number of array elements that have been set
      int numberOfFlags = 0;
      // check all the elements of the flag array
      for (int j = 0; j < numOfPivots; j++) {
        // when an element is set then we sum the total number of
        // flags set
        if (flagArray[j] == 1) {
          numberOfFlags++;
        }
      }

      if (numberOfFlags == numOfPivots) {
        // We have found flags for all pivots
        break;
      }
    }
    return flagArray;
  } // end computeFlagArray

  private static long computeMinFlag(int[] flagArray) {
    long minimumFlag = -1; // initialize minFlag

    for (int i = 0; i < flagArray.length; i++) {
      if (flagArray[i] == 1) {
        minimumFlag = i;
        break;
      }
    }
    return minimumFlag;
  } // end computeMinFlag
} // end FirstRound class
