import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class NextRound {
  private static final Logger log = LogManager.getLogger(NextRound.class);

  public static class NextRoundMap extends Mapper<LongWritable, Text, LongWritable, VectorElem> {
    // each partition has sampled the same number of points randomly
    // the pivots group for the current partition
    private static ArrayList<VectorElem> pivotPoints; // changed by rct on 201260122
    private static String pivotsPath;
    private static double epsilon;
    private static int numOfPivots;
    private static int vectorDimension;
    private static int iteration;

    protected void setup(Context context) throws IOException {
      log.setLevel(Level.OFF);
      log.debug("Starting NextRound.Map.setup:");

      Configuration conf = context.getConfiguration();

      pivotsPath = conf.get("currentPivotsPath", "THIS_WENT_WRONG");
      vectorDimension = conf.getInt("vectorSize", 0);
      iteration = conf.getInt("Iteration", 0);
      epsilon = conf.getDouble("eps", 0);
      pivotPoints = new ArrayList<>();

      System.out.println("Eps Next Map setup " + epsilon);
      System.out.println("Vector size Next Map setup " + vectorDimension);
      System.out.println("Iteration " + iteration);

      BufferedReader pivotsBuffer = new BufferedReader(new FileReader(pivotsPath));
      pivotsBuffer.lines().forEach(line -> pivotPoints.add(new VectorElem(line, vectorDimension)));
      pivotsBuffer.close();

      numOfPivots = pivotPoints.size();

      System.out.println(" --Pivot Points: " + numOfPivots);
      log.debug("NextRound.Map.setup complete.");
    } // end setup

    public void map(LongWritable inputKey, Text inputValue, Context context)
        throws IOException, InterruptedException {
      log.setLevel(Level.OFF);
      log.debug("Starting NextRoundMap.map:");

      // pivPts and eps have been establisheds in the setup
      // create the element that is being worked on in this map
      VectorElem currentVector = new VectorElem(inputValue.toString(), vectorDimension, true);
      VectorElem currentPivot; // tracks the current pivot point
      VectorElem closestPivot; // current best pivot point/partition

      // current min distance between elem and closestPivot
      double minimumDistance;
      // get the right pivots from pivotsGroup using the last assigned id
      Iterator<VectorElem> pivotsIterator = pivotPoints.iterator();
      if (pivotsIterator.hasNext()) {
        // establish a first closestPivot point
        closestPivot = pivotsIterator.next();
        minimumDistance = currentVector.getDistanceBetween(closestPivot);
      } else {
        // error if none found
        log.error(" --map Error: Distributed cache is empty");
        return;
      }

      while (pivotsIterator.hasNext()) { // find the closestPivot to this vec
        currentPivot = pivotsIterator.next();
        double currentDistance = currentVector.getDistanceBetween(currentPivot);
        // Then the current partition is closer, and it is the new
        // closestPivot
        if (currentDistance < minimumDistance) {
          closestPivot = currentPivot; // update closestPivot
          minimumDistance = currentDistance; // update current distance
        }
      }
      // set element assigned partition
      currentVector.setAssignedPartition(closestPivot.getKey());
      // set element base/ partition
      currentVector.setBasePartition(closestPivot.getKey());
      // Output <closestPivot, {id, vec, closestPivot, closestPivot}>;
      context.write(new LongWritable(closestPivot.getKey()), currentVector);
      log.debug(
          " --(closestPivot, vec) written to file: "
              + closestPivot.getKey()
              + ","
              + currentVector.toStringPart());

      // Next:: Process the points that located near the partition line
      // there are repeat distance computation
      pivotsIterator = pivotPoints.iterator();
      while (pivotsIterator.hasNext()) {
        // Now consider duplicates
        // get a current working pivot
        currentPivot = pivotsIterator.next();
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
          log.debug(
              " --(currentPivot, vec) written to file: "
                  + currentPivot.getKey()
                  + ","
                  + currentVector.toStringAll());
        }

        log.debug("NextRoundMap.map complete.");
      }
    } // end map
  } // end Map

  // ------------------------Reduce-----------------------------------//
  public static class NextRoundReduce extends Reducer<LongWritable, VectorElem, Text, Text> {

    private static ArrayList<VectorElem> pivotPoints; // changed by rct on 201260122
    private static String pivotsPath;
    private static double epsilon;
    private static int numOfPivots;
    private static int vectorSize;
    private static int iteration;
    private static int partitionUpperBound;
    private static String inputPath;
    private static String numbersPath;

    protected void setup(Context context) throws IOException {
      log.setLevel(Level.OFF);
      log.debug("Starting NextRound.Reduce.setup:");

      Configuration conf = context.getConfiguration();
      inputPath = conf.get("InputDir");
      numbersPath = conf.get("numbersInParDir");

      pivotsPath = conf.get("currentPivotsPath", "THIS_WENT_WRONG");
      epsilon = conf.getDouble("eps", 0);
      vectorSize = conf.getInt("vectorSize", 0);
      iteration = conf.getInt("Iteration", 0);
      pivotPoints = new ArrayList<>();
      partitionUpperBound = conf.getInt("PartitionUpBound", 0);

      System.out.println("Vector size Reduce setup " + vectorSize);
      BufferedReader pivotsBuffer = new BufferedReader(new FileReader(pivotsPath));
      pivotsBuffer.lines().forEach(line -> pivotPoints.add(new VectorElem(line, vectorSize)));
      pivotsBuffer.close();

      System.out.println("Printing the values of pivotsGroup in Next Reduce");
      pivotPoints.stream().forEach(System.out::println);

      numOfPivots = pivotPoints.size();
      System.out.println(" --Pivot Points: " + numOfPivots);
      log.debug("NextRound.Reduce.setup complete.");
    } // end setup

    public void reduce(LongWritable key, Iterable<VectorElem> values, Context context)
        throws IOException, InterruptedException {

      log.setLevel(Level.OFF);
      log.debug("Starting NextRoundReduce.reduce:");
      Configuration conf = context.getConfiguration();
      // eps is established in the reduce Setup
      // create a list to hold the records
      ArrayList<VectorElem> recordsList = new ArrayList<>();
      int recordCount = 0; // Record count

      // iterate values from map
      Iterator<VectorElem> valuesIterator = values.iterator();
      while (valuesIterator.hasNext()) {
        // pointer element
        VectorElem pointerElement = new VectorElem(valuesIterator.next());
        // log.debug(" --pointerElem: " + ptrElem.toString());

        // current element
        VectorElem currrentElement = new VectorElem(pointerElement);

        // add current element to records list
        recordsList.add(currrentElement);
        log.debug(" --RecordCount " + recordCount + " | currElem: " + recordsList.get(recordCount));
        recordCount++;
      }
      log.debug(
          "Record count:"
              + recordCount
              + "; PUB:"
              + partitionUpperBound
              + ";In dir: "
              + inputPath
              + iteration);

      // if (RdCount > PUB) {
      if (recordCount > partitionUpperBound && numOfPivots > 1) {
        // RecordCount is larger than partition threshold && #ofPivots > 1
        log.debug(" --RdCount > PUB: " + recordCount + "|" + partitionUpperBound);
        log.info("***** outputting for next round ****");
        // write our for next round job
        // write out the number of points in the partition
        // add by rct on 20160122
        Path outNumber =
            new Path(
                numbersPath + String.valueOf(iteration + 1) + "/" + key + "/" + key.toString());
        log.debug("    -outNumber path: " + outNumber);
        FileSystem fsTmp = FileSystem.get(conf);
        FSDataOutputStream outTmp = fsTmp.create(outNumber);
        outTmp.writeBytes(String.valueOf(recordCount));
        outTmp.close();
        log.debug("    -outNumber file written.");
        // out put file path is
        // someInputDir/String.valueof(Iter+1)/filename
        // one output partition in one folder  key/key
        Path outPath =
            new Path(inputPath + String.valueOf(iteration + 1) + "/" + key + "/" + key.toString());
        // write the large partition to hdfs file
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream out = fs.create(outPath);
        log.debug(" --Next iteration input file created: " + outPath);
        // Write to HDFS file
        for (VectorElem ve : recordsList) {
          out.writeBytes(
              ve.key
                  + ","
                  + ve.toStringAggElems()
                  + ","
                  + ve.parHistory
                  + "|"
                  + ve.assignedPartition
                  + "_"
                  + ve.basePartition
                  + "\n");
          log.debug(
              "Record written: "
                  + ve.key
                  + ","
                  + ve.toStringAggElems()
                  + ","
                  + ve.parHistory
                  + "|"
                  + ve.assignedPartition
                  + "_"
                  + ve.basePartition);
        }
        out.close();

      } else { // Modify to happen when conditions met OR #ofPivots == 1 (disregard PUB threshold)
        log.debug(" --RdCount !> PUB: " + recordCount + "|" + partitionUpperBound);
        log.info("*****output the cluster****");
        // create a list of clusters
        ArrayList<Cluster> clusters = createClusters(epsilon, recordsList, vectorSize);
        // for each cluster C in clusters
        for (int i = 0; i < clusters.size(); i++) {
          Cluster cluster = clusters.get(i);
          // rewrite the implementation
          Vector<Integer> minBaseHistory =
              getMinBaseHistory4AllRounds(cluster, iteration, numOfPivots);
          Vector<Integer> AssignedHistory = getAssignedParIdHistory(cluster);
          boolean check4out = check4Out(AssignedHistory, minBaseHistory);

          if (check4out == true) {
            context.getCounter("Groups_Identified", "counter").increment(1);
            
            log.debug("    -check4out == true");
            // Output cluster
            Text outKey = new Text("========== \t" + cluster.getClusterId());
            Text outElem = new Text("==========");
            context.write(outKey, outElem);
            log.debug("    -context.write(outKey,outElem): " + outKey + "," + outElem);
            // Output each element of the cluster
            log.debug("    -Outputting cluster");
            for (int j = 0; j < cluster.size(); j++) {
              VectorElem element = cluster.getVectorElem(j);
              outKey = new Text("" + element.key);
              // outElem = new Text("" + element.toStringElems());
              outElem = new Text("" + element.toStringAggElems());
              context.write(outKey, outElem);
              log.debug("    -context.write(outKey,outElem): " + outKey + "," + outElem);
            } // end for output each element of the cluster
          } // end if output cluster
        } // end for each cluster C in clusters
      } // end else
      log.debug("NextRoundreduce.reduce complete.");
    } // end reduce
  } // end NextRoundReduce

  // get the last assigned id for each point from its history
  public static String getAssignedParId(VectorElem point) {
    log.setLevel(Level.OFF);
    log.debug("  ~getAssignedParId:");
    String parId = null;
    String lastParIdPair = null;
    String parHistory = point.parHistory;
    // check if has gone through more than two rounds
    int last = parHistory.lastIndexOf('|');
    lastParIdPair = parHistory.substring(last + 1);

    String[] ids = lastParIdPair.split("_");
    parId = ids[0]; // get assigned id
    // System.out.println("test the last history: "+lastParIdPair+"  the assigned Id:"+ parId);
    log.debug("    -parId: " + parId);
    log.debug("  ~~getAssignedParId complete.");
    return parId;
  } // getAssignedParId

  public static ArrayList<Cluster> createClusters(
      double eps, ArrayList<VectorElem> dataPoints, int vectorSize) {

    log.setLevel(Level.OFF);
    log.debug("  ~CreateClusters called:");
    // System.out.println("DBG: createClusters(NxtRnd) called...");

    // create a list to hold the clusters
    ArrayList<Cluster> clusterList = new ArrayList<>();

    // for each point p in S
    for (int i = 0; i < dataPoints.size(); i++) {
      VectorElem elem = dataPoints.get(i);
      log.debug("    -elem from dataPoints.get(i): (" + elem.toStringPart() + ")");

      // set flag - each data point needs to be in a cluster
      boolean clusterFound = false;
      // set flag - initially assume points are not withinEps
      boolean withinEpsilon = false;

      // for each cluster Ci in C
      for (int j = 0; j < clusterList.size(); j++) {
        if (clusterList.isEmpty()) {
          log.debug("    -(" + elem + "): No clusters yet");
          break;
        }

        Cluster currentCluster = clusterList.get(j);
        // filter valid for vector data
        if (elem.getDistanceBetween(currentCluster.getCentroid()) > eps) {
          // p does not belong in this cluster
          log.debug(
              "    -Current cluster|centroid: "
                  + currentCluster.getClusterId()
                  + "|("
                  + currentCluster.getCentroid()
                  + ")");
          log.debug("    -(" + elem + "): Distance from pivot is more than epsilon");
        } else {
          // p may or may not belong to Ci
          // p is within eps of Ci centroid.
          withinEpsilon = true;

          // Now verify all points in C1 are within eps of p
          // for each VectorElem x in Ci
          for (int k = 0; k < currentCluster.size(); k++) {
            // holds current VectorElem x in Ci
            VectorElem clusterElement = currentCluster.getVectorElem(k);
            // p does not belong to Ci
            if (elem.getDistanceBetween(clusterElement) > eps) {
              // System.out.println(elem + " Distance from other points is more than epsilon");
              log.debug("    -clusterElem (key): " + clusterElement.getKey());
              log.debug("    -(" + elem + "): Distance from other points is more than epsilon");
              withinEpsilon = false;
              break;
            } // end if p does not belong to Ci
          } // end for each VectorElem x in Ci

          if (withinEpsilon) {
            // p belongs to Ci
            currentCluster.addVector(elem);
            // currentCluster.updateCentroid();
            // this could be done in the cluster
            clusterFound = true;
            // System.out.println(elem + " I was added to a cluster");
            log.debug("    -(" + elem + "): I was added to a cluster");
            break;
          } // end if p belongs to Ci
        } // end else p may or may not belong to Ci
      } // end for each cluster Ci in C

      if (!clusterFound) {
        // we did not find a cluster for p
        // then p must belong to a new cluster
        Cluster newCluster = new Cluster(vectorSize);
        newCluster.addVector(elem);
        clusterList.add(newCluster); // add new cluster to cluster list
        // System.out.println(elem + " New cluster created");
        log.debug("    -New cluster created from (" + elem + ")");
      } // end if we did not find a cluster for p
    } // end for each point p in S

    // DBG: output cluster contents
    for (int w = 0; w < clusterList.size(); w++) {
      //            System.out.println("DBG: Clusters -- ");
      //            System.out.println(clusterList.get(w).getCluster());
      log.debug("    -Clusters: ");
      log.debug(clusterList.get(w).getCluster());
    }

    //        System.out.println("There are " + clusterList.size() + " clusters");
    log.debug("    -There are " + clusterList.size() + " clusters.");

    return clusterList;
  } // end createClusters

  /*
   * computeFlagArray (Cluster) Int flagArray= new int[numOfPivots] //the
   * number of numOfPivots will be known to all the reducers For each record
   * in Cluster int recordPartition=record.getBasePartition(); //Grab the Base
   * Partition to which this record belongs and assign the corresponding array
   * position to 1 flagArray[recordPartition]=1; If all elements of flagArray
   * are 1 break;
   */
  /**
   * Changed by rct 2016-01-07 the returned value in flagArray is the minimum index in each round
   */
  public static int[] computeFlagArray(Cluster cluster, int numOfPivots) {
    // the number of numOfPivots will be know to all the reducers
    // add by rct on 2016-01-07
    log.setLevel(Level.OFF);
    log.debug("  ~computeFlagArray(cluster) <NxtRnd> called:");

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
    log.debug("  ~~computeFlagArray(cluster) complete.");
    return flagArray;
  } // end computeFlagArray

  public static long computeMinFlag(int[] flagArray) {
    log.debug("computeMinFlag(cluster) called <NxtRnd>:");
    long minFlag = -1; // initialize minFlag

    for (int i = 0; i < flagArray.length; i++) {
      if (flagArray[i] == 1) {
        minFlag = i;
        break;
      }
    }
    log.debug(" computeMinFlag(flagArray) completed. MinFlag returned: " + minFlag);
    return minFlag;
  } // end computeMinFlag

  /**
   * By rct 2016-01-07 get the minimum base partition number of all the points in one cluster, and
   * stored in a vector
   *
   * @param cluster
   * @return
   */
  public static Vector<Integer> getMinBaseHistory4AllRounds(
      Cluster cluster, int Iteration, int numOfPivots) {
    log.setLevel(Level.OFF);
    log.debug("  ~getMinBaseHistory4AllRounds(cluster) called:");
    Vector<Integer> MinBaseParHistory = new Vector<>();

    // temporary variable[] to get minimum values
    int[] minBasePar = new int[Iteration + 1];

    // initialize
    for (int i = 0; i < Iteration; i++) {
      minBasePar[i] = numOfPivots + 1; // ensure this is the largest value
    }
    int count = cluster.size();
    for (int i = 0; i < count; i++) {
      VectorElem record = cluster.getVectorElem(i);

      String parHistory = record.parHistory;
      // System.out.println("parHistory: "+parHistory);
      // split the history group using '|'
      StringTokenizer tokens = new StringTokenizer(parHistory, "|");
      int sequence = 0;
      while (tokens.hasMoreTokens()) {
        String Assigned_Base = tokens.nextToken();
        // System.out.println("Assigned_Base: "+Assigned_Base);
        // split the single history with "_"
        String[] numbers = Assigned_Base.split("_");
        int baseId = Integer.parseInt(numbers[1]);
        // System.out.println("baseId: "+baseId);
        // check if the value is the minimum
        if (minBasePar[sequence] > baseId) minBasePar[sequence] = baseId;
        sequence = sequence + 1;
      }
    }
    for (int i = 0; i < Iteration + 1; i++) MinBaseParHistory.add(minBasePar[i]);
    log.debug("  ~~getMinBaseHistory4AllRounds(cluster) complete.");
    return MinBaseParHistory;
  } // end getMinBaseHistory4AllRounds method

  /**
   * BY rct 2016-01-07 AS the points in the same cluster/group has the same assigned partition
   * history just get the assigned partition id history from the first point process one point
   *
   * @param cluster
   * @return
   */
  public static Vector<Integer> getAssignedParIdHistory(Cluster cluster) {
    log.setLevel(Level.OFF);
    log.debug("  ~getAssignedParIdHistory(cluster) called:");
    log.debug("    -input clusterID: " + cluster.getClusterId());

    Vector<Integer> assignedParId = new Vector<>();
    VectorElem record = cluster.getVectorElem(0);
    String assignedParHistory = record.parHistory;
    log.debug("    -VE record: " + record.toStringAll());
    log.debug("    -assignedParHistory: " + assignedParHistory);

    // split the history group using '|'
    StringTokenizer tokens = new StringTokenizer(assignedParHistory, "|");
    while (tokens.hasMoreTokens()) {
      String history = tokens.nextToken();
      // split the single history with "_"
      String[] numbers = history.split("_");
      // get the assigned value
      int assignedPar = Integer.parseInt(numbers[0]);
      assignedParId.add(assignedPar);
      log.debug("    -parsed assignedPar added to assignedParID list: " + assignedPar);
    }
    log.debug("  ~~getAssignedParIdHistory(cluster) complete.");
    return assignedParId;
  } // end getAssignedParIdHistory method
  /**
   * by rct 2016-01-07 check the assigned partition id history and the minimum base partition id
   * history
   *
   * @param assigned: the assigned partition id history for the group
   * @param miniBase: the minimum partition id of points in one group for all execution rounds each
   *     value for one round
   * @return
   */
  public static boolean check4Out(Vector<Integer> Assigned, Vector<Integer> miniBase) {
    log.setLevel(Level.OFF);
    log.debug("  ~check4Out(Assigned,miniBase) called:");
    log.debug("    -Assigned: " + Assigned);
    log.debug("    -miniBase: " + miniBase);

    int count = Assigned.size();
    log.debug("    -count: " + count);
    int i; // Jeremy changed_4.17.18: Removed initialization; gets done by if statement
    for (i = 0; i < count; i++) {
      // if(Assigned.get(i)==miniBase.get(i)){
      if (Assigned.get(i)
          .equals(miniBase.get(i))) { // Changed by Jeremy_4.17.18: Better comparison for non-string
        i++;
        log.debug("    -Assigned(i).equals(miniBase(i). i++ = " + i);
      } else {
        break;
      }
    }
    if (i == count) {
      log.debug("  ~~ i == count. check4Out complete [return true].");
      return true;
    } else log.debug("  ~~ i != count. check4Out complete [return false].");
    return false;
  } // end check4Out method
} // end NextRound class
