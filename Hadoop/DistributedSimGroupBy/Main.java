import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class Main extends Configured implements Tool {

  private static final Logger log = LogManager.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Main(), args);
    System.exit(res);
  } // end main

  // @Override
  public int run(String[] args) throws Exception {
    log.setLevel(Level.OFF);

    if (args.length < 4) {
      System.out.println(
          "Usage: bin/hadoop jar MRSGB.jar Main <eps> <number of reduces> <number pivots> <vector size>");
      return -1;
    }

    /*
     * SGB global variables.
     */
    double epsilon;
    int numReduces;
    int numberOfPivots;
    int vectorSize;
    double pivotsRatio = 0;
    int partitionsUpperBound = 50000;

    try {
      epsilon = Double.parseDouble(args[0]);
      System.out.println("eps set to: " + epsilon);
    } catch (NumberFormatException e) {
      System.out.println(
          "Usage: bin/hadoop jar MRSGB.jar Main <eps> <number of reduces> <number pivots> <vector size>");
      System.out.println("Error: eps not a type double");
      return -1;
    }

    try {
      numReduces = Integer.parseInt(args[1]);
      System.out.println("number reducers set to: " + numReduces);
    } catch (NumberFormatException e) {
      System.out.println(
          "Usage: bin/hadoop jar MRSGB.jar Main <eps> <number of reduces> <number pivots> <vector size>");
      System.out.println("Error: number of reduces not a type integer");
      return -1;
    }

    try {
      numberOfPivots = Integer.parseInt(args[2]);
      System.out.println("number of pivots set to: " + numberOfPivots);
    } catch (NumberFormatException e) {
      System.out.println(
          "Usage: bin/hadoop jar MRSGB.jar Main <eps> <number of reduces> <number pivots> <vector size>");
      System.out.println("Error: number of pivots not a type integer");
      return -1;
    }

    try {
      vectorSize = Integer.parseInt(args[3]);
      System.out.println("Element Length set to: " + vectorSize);
    } catch (NumberFormatException e) {
      System.out.println(
          "Usage: bin/hadoop jar MRSGB.jar Main <eps> <number of reduces> <number pivots> <vector size>");
      System.out.println("Error: vector size number not a type integer");
      return -1;
    }

    // Allow for pivot ratio to entered as parameter
    if (args.length == 5) {
      try {
        pivotsRatio = Double.parseDouble(args[4]);
        System.out.println("Pivot Ratio set to: " + pivotsRatio);
      } catch (NumberFormatException e) {
        System.out.println(
            "Usage: bin/hadoop jar MRSGB.jar Main <eps> <number of reduces> <number pivots> <vector size>");
        System.out.println("Error: pivot ratio not a type double");
        return -1;
      }
    }

    if (args.length == 6) {
      try {
        partitionsUpperBound = Integer.parseInt(args[5]);
        System.out.println("partitionsUpperBound set to: " + partitionsUpperBound);
      } catch (NumberFormatException e) {
        System.out.println(
            "Usage: bin/hadoop jar MRSGB.jar Main <eps> <number of reduces> <number pivots> <vector size>");
        System.out.println("Error: upperbound size number not a type double");
        return -1;
      }
    }
    int iteration = 0;
    Configuration conf = this.getConf();
    StopWatch sWatch = new StopWatch();
    sWatch.start();
    long startTime = System.nanoTime();

    /*
     * Establish connection to HDFS and set paths for SGB in
     * order to facilitate easy clean up after completion.
     */
    FileSystem hdfs = FileSystem.get(conf);

    // Appending the name of the current HDFS to each directory path for clean up.
    String fsURIString = hdfs.getUri().toString();

    /*
     * SGB Hadoop assumes that the initial input data is stored
     * as /InputDir/Iter0/some_data_files. This appears to pose to
     * no issue for the random sampler if the files are simply all dumped
     * into one directory, in this case, /InputDir/Iter0.
     */
    String inputParentPath = fsURIString + "/InputDir";
    String outputParentPath = fsURIString + "/OutputDir";
    String pivotsParentPath = fsURIString + "/PivotsDir";
    String numbersParentPath = fsURIString + "/NumbersDir";

    //    String inputParentPath = "InputDir";
    //    String outputParentPath = "OutputDir";
    //    String pivotsParentPath = "PivotsDir";
    //    String numbersParentPath = "NumbersDir";

    String pivotsSymLink;

    String inputDir = inputParentPath + "/Iter";
    String outputDir = outputParentPath + "/Iter";
    String pivDir = pivotsParentPath + "/Iter";
    String numbersInParDir = numbersParentPath + "/Iter";

    long memory = 33554432; // 32MB
    double ratio;

    if (pivotsRatio != 0) {
      ratio = pivotsRatio;
    } else {
      ratio = 0.1; // 0.1; // May need to do in fractions of 1%, not 100%
    }
    // double ratio = 0.05; //0.1; // May need to do in fractions of 1%, not 100%

    String parameters =
        "Number of Reducers: "
            + numReduces
            + "\nDimension: "
            + vectorSize
            + "\nNumber of Pivots: "
            + numberOfPivots
            + "\nThreshold: "
            + partitionsUpperBound
            + "\nEpsilon: "
            + epsilon
            + "\nFrequency: "
            + pivotsRatio
            + "\nInput Path: "
            + inputParentPath
            + "\nOutput Path: "
            + outputParentPath;

    // Hadoop/MR configs set by someone else.
    conf.setBoolean("mapred.output.compress", false);
    conf.set(
        "mapreduce.child.java.opts", "-Xmx1024M"); // Updated for v2.7.3, increased size from 512M
    conf.set("mapreduce.task.timeout", "8000000");
    conf.set("memory", Long.toString(memory)); // assigns the variable mem to conf

    /*
     * Global config parameters.
     */
    // Time runtime and record to local file
    conf.set("InputDir", inputDir);
    conf.set("pivDir", pivDir);
    conf.set("numbersInParDir", numbersInParDir);

    conf.setDouble("eps", epsilon); // assign string for eps to conf
    conf.setInt("vectorSize", vectorSize); // assigns vector dimensionality (# elements) to conf
    conf.setInt(
        "numberOfPivots", numberOfPivots); // assigns vector dimensionality (# elements) to conf
    conf.setInt("PartitionUpBound", partitionsUpperBound); // FOR TESTING

    long groupsIdentifiedCounter = 0;
    while (true) {
      conf.setInt("Iteration", iteration);
      log.debug("MRSGB | Iteration: " + String.valueOf(iteration));
      System.out.println("MRSGB | Iteration: " + String.valueOf(iteration));

      // the input path the initial input path is someInputDir/Iter0/
      // under each Iter_i, there are some folder/partition for next process

      Path inputDirPath = new Path(inputDir + String.valueOf(iteration) + "/");
      System.out.println(inputDirPath);
      log.debug(" --inputDirPath: " + inputDirPath);

      // check if there is new partition need next round to process
      // if not exist the whole process is over
      if (!hdfs.exists(inputDirPath)) {
        log.debug("No New Partition To Process. Success Over!");
        // System.out.println("No New Partition To Process. Success Over!");
        break;
      }

      if (iteration > 0) { // Call NextRound; FirstRound has been completed and was insufficient
        log.setLevel(Level.OFF);
        log.debug("Main.run_NewRoundLogic | Iteration " + iteration);

        // traverse the input folder to get the partition name, in which the file with the same name
        FileStatus status[] = hdfs.listStatus(inputDirPath);
        for (FileStatus st : status) {
          log.debug(" --input partition/filename for sample: " + st.getPath().getName());
          String infilename = st.getPath().getName();
          log.debug(" --Iter>0 inputfilename: " + infilename);
          // set out put path
          Path outPath = new Path(outputDir + String.valueOf(iteration) + "/" + infilename + "/");
          log.debug(" --Iter>0 outPath: " + outPath);
          // Path to the current set of pivots that will be placed in the distributed cache.
          Path pivPath =
              new Path(pivDir + String.valueOf(iteration) + "/pivots_" + infilename + ".txt");

          // Symbolic link to the pivots path.
          pivotsSymLink = "Iter" + iteration + "_pivots_" + infilename;
          conf.set("currentPivotsPath", pivotsSymLink);
          log.debug(" --       pivPath: " + pivPath);

          // generate pivots for each input partition
          Path partitionInputPath =
              new Path(inputDir + String.valueOf(iteration) + "/" + infilename + "/");
          log.debug(" --       partInPath: " + partitionInputPath);

          // compute how many pivots will be selected
          Path numInPar =
              new Path(
                  numbersInParDir
                      + String.valueOf(iteration)
                      + "/"
                      + infilename
                      + "/"
                      + infilename);

          int No2Select = getPivotsNumber(numInPar, ratio, hdfs);
          Object[] pivots = getPivots(partitionInputPath, No2Select);
          conf.setInt(
              "numberOfPivots",
              pivots.length); // Added 6.6.18_Silva/Jeremy: added to properly set global variable

          // write pivots to file named with "/pivots_" + infilename +".txt"
          FSDataOutputStream out = hdfs.create(pivPath);

          for (int i = 0; i < pivots.length; i++) {
            // pre-process the input pivots
            String inline = ((Text) pivots[i]).toString();
            log.debug(" --inputPvt(inline) #" + i + ": " + inline);

            VectorElem tempElement = new VectorElem(inline, vectorSize, true);
            // log.debug("  --creating tempElem " + i + " from: " + pivots[i].toString());
            // log.debug(" --tempElem (inline,vSize,fr): " + tempElem.toStringPart());

            // System.out.println(tempBuf.toString());
            tempElement.setKey((long) i);
            out.writeBytes(tempElement.toString() + '\n');
            // log.debug("  --tempElem written to file: " + tempElem.toString());
            // System.out.println("  -- tempElem (FR): " + tempElem.toStringAll());
          }
          out.close();

          // creates job
          log.info(
              "MRSGB: Iteration " + String.valueOf(iteration) + " for Partition: " + infilename);

          Job job = Job.getInstance(conf);
          job.setJobName("MRSGB: Iter #" + String.valueOf(iteration) + " | Part #" + infilename);

          // set the number of reduce tasks from input parameter
          job.setNumReduceTasks(numReduces);
          job.setJarByClass(Main.class);
          // add a group of files to cache
          job.addCacheFile(new URI(pivPath + "#" + pivotsSymLink));

          // sets Next Round Mapper class
          job.setMapperClass(NextRound.NextRoundMap.class);
          // Set Next Round Reducer class
          job.setReducerClass(NextRound.NextRoundReduce.class);
          // sets map output key/value types
          job.setMapOutputKeyClass(LongWritable.class);
          job.setMapOutputValueClass(VectorElem.class);

          // specify output key/value types
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(Text.class);

          // sets output format
          job.setInputFormatClass(TextInputFormat.class);

          // specify input and output DIRECTORIES (not files)
          FileInputFormat.addInputPath(job, partitionInputPath); // by rct 2016-01-08
          FileOutputFormat.setOutputPath(job, outPath);

          // start and wait till it finishes
          job.waitForCompletion(true);
        } // end for
      } else { // Initial run, call FirstRound
        log.setLevel(Level.OFF);
        log.debug("Entering Main.run | FirstRound Logic");

        // pivot points generation for the first round job
        Path outPath = new Path(outputDir + String.valueOf(iteration) + "/");

        log.debug(" --FirstRound outPath: " + outPath);

        Object[] pivots = getPivots(inputDirPath, numberOfPivots);

        // write pivots to file
        Path pivPath = new Path(pivDir + String.valueOf(iteration) + "/pivots.txt");
        // Symbolic link to pivots in distributed cache.
        pivotsSymLink = "Iter" + iteration + "_pivots.txt";
        conf.set("currentPivotsPath", pivotsSymLink);

        FSDataOutputStream out = hdfs.create(pivPath);
        log.debug(" --FirstRound pivots file created: " + pivPath);
        for (int i = 0; i < pivots.length; i++) {
          VectorElem tempElem = new VectorElem(((Text) pivots[i]).toString(), vectorSize);
          tempElem.setKey((long) i);
          out.writeBytes(tempElem.toString() + '\n');
          log.debug(" --pivPt written to file: " + tempElem.toString());
        }
        out.close();

        log.info("MRSGB: Iteration " + String.valueOf(iteration) + " for Partition 0");
        Job job = Job.getInstance(conf);
        job.setJobName("MRSGB: Iter #" + String.valueOf(iteration) + " | Part #0");

        // set the number of reduce tasks from input parameter
        job.setNumReduceTasks(numReduces);
        job.setJarByClass(Main.class);

        // add file to cache
        job.addCacheFile(new URI(pivPath + "#" + pivotsSymLink));

        // sets mapper class
        job.setMapperClass(FirstRound.FirstMap.class);

        // Set Reducer class
        job.setReducerClass(FirstRound.FirstReduce.class);

        // sets map output key/value types
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(VectorElem.class);

        // specify output key/value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // sets output format
        job.setInputFormatClass(TextInputFormat.class);

        // specify input and output DIRECTORIES (not files)
        FileInputFormat.addInputPath(job, inputDirPath);
        FileOutputFormat.setOutputPath(job, outPath);

        // start and wait till it finishes
        log.debug("Launching FirstRound | main/job.waitFC()");
        job.waitForCompletion(true);

        groupsIdentifiedCounter +=
            job.getCounters().findCounter("Groups_Identified", "counter").getValue();
      }

      log.info("**Iteration**: " + iteration + " complete.");
      iteration++;
    }

    // Stop stopWatch; output time to console and file
    long endTime = System.nanoTime();
    sWatch.stop();

    log.info("Total Run Time: " + (endTime - startTime));
    log.info("MRSGB Total run time: " + sWatch.toString());

    String date =
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")).toString();

    String executionTimeStr = "Execution time (ns): " + (endTime - startTime);
    String numberGroupsStr = "Groups Identified: " + groupsIdentifiedCounter;
    String iterationsStr = "Iterations: " + iteration;
    String fileName = "HadoopSGB_" + date + ".txt";
    /*
     * writeUTF throws away the first line for some reason.
     * Easiest solution was to add a new line character at the beginning of the string.
     */
    String summary =
        "Date_Time: "
            + date
            + "\n"
            + parameters
            + "\n"
            + iterationsStr
            + "\n"
            + numberGroupsStr
            + "\n"
            + executionTimeStr;

    System.out.println("Summary: \n" + summary);

    Path statsPath = new Path(hdfs.getUri() + "/stats/Stats_" + fileName);
    FSDataOutputStream statsFile = hdfs.create(statsPath);
    statsFile.writeUTF(summary);
    statsFile.close();

    // Delete output for convience.
    // hdfs.delete(new Path(pivotsParentPath), true);
    // hdfs.delete(new Path(numbersParentPath), true);
    // hdfs.delete(new Path(outputParentPath), true);

    // for (int i = 1; i < iteration; i++) {
    //  Path directoryToDelete = new Path(inputParentPath + "/Iter" + i);
    //  hdfs.delete(directoryToDelete, true);
    // }

    hdfs.close();
    return 0;
  } // end run

  Object[] getPivots(Path input, int numPivs) throws IOException {
    log.setLevel(Level.OFF);
    System.out.println("  ~getPivots called...");
    System.out.println("    -input: " + input + " | numPivs: " + numPivs);

    JobConf job = new JobConf();
    job.setInputFormat(KeyValueTextInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    org.apache.hadoop.mapred.FileInputFormat.addInputPath(job, input);
    final KeyValueTextInputFormat inf = (KeyValueTextInputFormat) job.getInputFormat();
    InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<>(1.0, numPivs, 100);
    Object[] samples = sampler.getSample(inf, job);
    // log.debug("  ~~getPivots complete.");
    System.out.println("  ~~getPivots complete.");
    return samples;
  } // end getPivots

  int getPivotsNumber(Path input, double ratio, FileSystem hdfs) throws IOException {
    log.setLevel(Level.OFF);
    System.out.println("  ~getPivotsNumber called (ratio = " + ratio + "):");
    if (!hdfs.exists(input)) throw new IOException("Input " + input + " file not found");
    if (!hdfs.isFile(input)) throw new IOException("Input " + input + " should be a file");

    FSDataInputStream in = hdfs.open(input);
    BufferedReader bufread = new BufferedReader(new InputStreamReader(in));
    String strLine = bufread.readLine();

    int No2Select =
        (int)
            (Integer.parseInt(strLine)
                * ratio); // Multiply # of partition elements by designated ratio

    if (No2Select == 0) { // Changed by Jeremy/Silva_5.30.18: from above
      No2Select = 1; // Changed by Jeremy/Silva5.30.18: from 2 (from previous NoSelect+1)
    }
    System.out.println("    -Returning " + No2Select + " pivots.");
    System.out.println("  ~~getPivotsNumber complete.");
    return No2Select;
  } // end getPivotsNumber
} // end main method
