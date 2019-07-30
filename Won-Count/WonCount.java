import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

/*
arg[0] = input path
arg[1] = temp path
arg[2] = output path
*/
public class WonCount {
  private static String myArg;
  public static enum MyCounter{TotalCount};

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String tagToken = itr.nextToken();
        String resultToken;
        if(tagToken.equals("[Result")){
          context.getCounter(MyCounter.TotalCount).increment(1);
          word.set(tagToken);
          context.write(word, one);;
        } 
      }
    }
  }

  public static class FinalMapper
       extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String tagToken = itr.nextToken();
        String resultToken;
        if(tagToken.equals("[Result")){
          resultToken = itr.nextToken();
          switch(resultToken.substring(1, resultToken.length()-2)){
            case "0-1":
              resultToken = "BLACK";
              break;
            case "1-0":
              resultToken = "WHITE";
              break;
            case "1/2-1/2":
              resultToken = "DRAW";
              break;
          }
          word.set(resultToken);
          context.write(word, one);
        } 
      }
    }
  }

  public static class MyCombiner
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable countResult = new IntWritable();
    private Text mutipleResult = new Text();
    private static float totalCounter;
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

      Configuration conf = context.getConfiguration();
      totalCounter = Float.parseFloat(conf.get("totalCount"));

      int sum = 0;
      float percentage;
      for (IntWritable val : values) {
        sum += val.get();
      }
      countResult.set(sum);
      context.write(key, countResult);
    }
  }

  public static class FinalReducer
       extends Reducer<Text,IntWritable,Text,Text> {
    //private IntWritable countResult = new IntWritable();
    private Text mutipleResult = new Text();
    private static float totalCounter;
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

      Configuration conf = context.getConfiguration();
      totalCounter = Float.parseFloat(conf.get("totalCount"));

      int sum = 0;
      float percentage;
      for (IntWritable val : values) {
        sum += val.get();
      }

      percentage = sum/totalCounter;
      String mutipleValue = sum + "\t" +percentage;
      mutipleResult.set(mutipleValue);
      context.write(key, mutipleResult);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "won count");
    job.setJarByClass(WonCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setNumReduceTasks(0);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);

    //get total game count
    Counters counters = job.getCounters();
    Counter total = counters.findCounter(MyCounter.TotalCount);
    float totalCounter = total.getValue();
    System.out.println(totalCounter);
    //set up job2 to compute percentage
    Configuration conf2 = new Configuration();
    conf2.set("totalCount", String.valueOf(totalCounter)); //pass to finalreducer
    Job job2 = Job.getInstance(conf2, "percentage");
    job2.setJarByClass(WonCount.class);
    job2.setMapperClass(FinalMapper.class);
    job2.setCombinerClass(MyCombiner.class);
    job2.setReducerClass(FinalReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job2, new Path(args[0]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));

    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}