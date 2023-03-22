import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HadoopGroupBy extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new HadoopGroupBy(), args);

    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    long t0 = System.nanoTime();

    Configuration conf = this.getConf();
    FileSystem hdfs = FileSystem.get(conf);

    int dimension = Integer.parseInt(args[0]);
    int aggregationChoice = Integer.parseInt(args[1]);
    int numberReducers = Integer.parseInt(args[2]);

    String pathToRead = args[3];
    String pathToWrite = args[4];

    // String pathToWrite = hdfs.getUri() + "/" + args[4];
    // String pathToWrite = hdfs.getUri() + "/" + args[4];

    String parameters =
        "Dimension: "
            + dimension
            + "\nReducers: "
            + numberReducers
            + "\nAggregation Choice: "
            + aggregationChoice
            + "\nInput Path: "
            + pathToRead
            + "\nOutput Path: "
            + pathToWrite;

    System.out.println(parameters);

    Path inputPath = new Path(pathToRead);
    Path outputPath = new Path(pathToWrite);

    conf.setInt("dimension", dimension);
    conf.setInt("aggregationChoice", aggregationChoice);

    Job groupByJob = Job.getInstance(conf);
    groupByJob.setJobName("GroupBy");
    groupByJob.setJarByClass(HadoopGroupBy.class);
    groupByJob.setMapperClass(GroupByMapper.class);
    groupByJob.setPartitionerClass(HashPartitioner.class);
    groupByJob.setReducerClass(GroupByReducer.class);
    groupByJob.setMapOutputKeyClass(GroupByVector.class);
    groupByJob.setMapOutputValueClass(DoubleWritable.class);
    groupByJob.setOutputKeyClass(Text.class);
    groupByJob.setOutputValueClass(Text.class);

    groupByJob.setNumReduceTasks(numberReducers);

    FileInputFormat.addInputPath(groupByJob, inputPath);
    FileOutputFormat.setOutputPath(groupByJob, outputPath);

    if (!groupByJob.waitForCompletion(true)) {
      return 1;
    }

    // Job execution time.
    long t1 = System.nanoTime();

    /*
     * Standard time format HH:mm:ss results in path issues. Had to switch to HH-mm-ss format to avoid errors.
     */
    String date =
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")).toString();

    long groupsIdentifiedCounter =
        groupByJob.getCounters().findCounter("Groups_Identified", "count").getValue();

    String executionTimeStr = "Execution time (ns): " + (t1 - t0);
    String numberGroupsStr = "Groups Identified: " + groupsIdentifiedCounter;

    String summary =
        "Date_Time: " + date + "\n" + parameters + "\n" + numberGroupsStr + "\n" + executionTimeStr;

    // Print the summary for convenience.
    System.out.println("Summary: \n" + summary);

    String fileName = "HadoopGroupBy_" + date + "_" + aggregationChoice + ".txt";
    Path statsPath = new Path(hdfs.getUri() + "/stats/Stats_" + fileName);
    FSDataOutputStream statsFile = hdfs.create(statsPath);
    statsFile.writeUTF(summary);
    statsFile.close();
    // Delete output directory for the convenience of submitting another experiment job.
    // hdfs.delete(outputPath, true);
    hdfs.close();
    return 0;
  }

  public static class GroupByMapper
      extends Mapper<LongWritable, Text, GroupByVector, DoubleWritable> {

    private int dimension;

    @Override
    public void setup(Context context) {
      dimension = context.getConfiguration().getInt("dimension", 0);
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      String[] csvEntries = value.toString().split(",");
      double[] feature = new double[dimension];
      double aggregateValue = Double.parseDouble(csvEntries[1]);
      for (int i = 0; i < dimension; i++) {
        feature[i] = Double.parseDouble(csvEntries[i + 2]);
      }
      context.write(new GroupByVector(feature), new DoubleWritable(aggregateValue));
    }
  }

  public static class GroupByReducer extends Reducer<GroupByVector, DoubleWritable, Text, Text> {

    private int aggregationChoice;

    private static double average(Iterable<DoubleWritable> values) {
      long count = 0;
      double sum = 0;
      for (DoubleWritable aggregateValue : values) {
        sum += aggregateValue.get();
        count++;
      }
      return sum / count;
    }

    private static long count(Iterable<DoubleWritable> values) {
      long count = 0;
      for (@SuppressWarnings("unused") DoubleWritable aggregateValue : values) {
        count++;
      }
      return count;
    }

    private static double minimum(Iterable<DoubleWritable> values) {
      double minimumValue = Double.MAX_VALUE;
      for (DoubleWritable aggregateValue : values) {
        if (aggregateValue.get() < minimumValue) {
          minimumValue = aggregateValue.get();
        }
      }
      return minimumValue;
    }

    private static double maximum(Iterable<DoubleWritable> values) {
      double maximumValue = Double.MIN_VALUE;
      for (DoubleWritable aggregateValue : values) {
        if (maximumValue < aggregateValue.get()) {
          maximumValue = aggregateValue.get();
        }
      }
      return maximumValue;
    }

    private static double sum(Iterable<DoubleWritable> values) {
      double sum = 0;
      for (DoubleWritable aggregateValue : values) {
        sum += aggregateValue.get();
      }
      return sum;
    }

    @Override
    public void setup(Context context) {
      aggregationChoice = context.getConfiguration().getInt("aggregationChoice", 0);
    }

    @Override
    public void reduce(GroupByVector key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {

      switch (aggregationChoice) {
        case 1:
          context.write(new Text(key.toString()), new Text(String.valueOf(average(values))));
          break;
        case 2:
          context.write(new Text(key.toString()), new Text(String.valueOf(count(values))));
          break;
        case 3:
          context.write(new Text(key.toString()), new Text(String.valueOf(maximum(values))));
          break;
        case 4:
          context.write(new Text(key.toString()), new Text(String.valueOf(minimum(values))));
          break;
        case 5:
          context.write(new Text(key.toString()), new Text(String.valueOf(sum(values))));
          break;
        default:
          System.out.println("Failure to read aggregation choice. Default aggregation is average");
          context.write(new Text(key.toString()), new Text(String.valueOf(average(values))));
      }
      context.getCounter("Groups_Identified", "count").increment(1);
    }
  }
}
