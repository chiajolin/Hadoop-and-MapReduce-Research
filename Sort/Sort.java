import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
arg[0] = input path
arg[1] = temp path
arg[2] = output path
*/
public class Sort {
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
    private Text player = new Text(); //key
    private Text opponent = new Text(); //value

    private String tagToken;
    private String playerToken;
    private String whitePlayer;
    private String blackPlayer;

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        tagToken = itr.nextToken();
 
        if(tagToken.equals("[White")){
          playerToken = itr.nextToken();
          whitePlayer = playerToken.substring(1,playerToken.length()-2);
          System.out.println(whitePlayer);
        } 

        if(tagToken.equals("[Black")){
          playerToken = itr.nextToken();
          blackPlayer = playerToken.substring(1,playerToken.length()-2);
        }

        if(tagToken.equals("[Result")){
          //set key value pair
          if(whitePlayer != null && !whitePlayer.isEmpty() && blackPlayer != null && !blackPlayer.isEmpty()){
          player.set(whitePlayer);
          opponent.set(blackPlayer);
          context.write(player, opponent);
          context.write(opponent, player);
          }

        }
      }
    }
  }

  //<key,value> -> <key, value + " " + value ....> 
  public static class MyCombiner
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      StringBuilder sb = new StringBuilder();
      for (Text val : values) {
        sb.append(val.toString());
        sb.append(" ");
      }
      result.set(sb.toString());
      context.write(key, result);
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int totalGame = 0;
      List<String> distinctOpponents = new ArrayList<String>();
      for (Text val : values) {
        String[] opponents = val.toString().split(" ");
        for(String opponent : opponents){
          totalGame ++;
          if(!distinctOpponents.contains(opponent)){
            distinctOpponents.add(opponent);
          }
        }
      }
      String multiResult = totalGame + "\t" + distinctOpponents.size();
      result.set(multiResult);
      context.write(key, result);
    }
  }

  public static class SortingMapper
       extends Mapper<Object, Text, MyWritable, NullWritable>{
    private MyWritable myWritable = new MyWritable();
    private NullWritable nullValue = NullWritable.get();
    private Text playerid = new Text();
    private IntWritable totalGame = new IntWritable();
    private IntWritable distinctOpponents = new IntWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        playerid.set(itr.nextToken());
        totalGame.set(Integer.parseInt(itr.nextToken()));
        distinctOpponents.set(Integer.parseInt(itr.nextToken()));
        myWritable.set(playerid, totalGame, distinctOpponents);
        context.write(myWritable, nullValue);
      }
    }
  }

  public static class SortingReducer
    extends Reducer<MyWritable, NullWritable, MyWritable, NullWritable> {
    public void reduce(MyWritable key, Iterable<NullWritable> values,
      Context context) throws IOException, InterruptedException {
    for (NullWritable value : values) {
      context.write(key, NullWritable.get());
    }

  }
}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "compute");
    job.setJarByClass(Sort.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(MyCombiner.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.waitForCompletion(true);

    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "sort");
    job2.setJarByClass(Sort.class);
    job2.setMapperClass(SortingMapper.class);
    job2.setMapOutputKeyClass(MyWritable.class);
    job2.setMapOutputValueClass(NullWritable.class);
    job2.setPartitionerClass(MyPartitioner.class);
    job2.setGroupingComparatorClass(GroupComparator.class);
    job2.setSortComparatorClass(MyComparator.class);
    job2.setReducerClass(SortingReducer.class);
    job2.setOutputKeyClass(MyWritable.class);
    job2.setOutputValueClass(NullWritable.class);

    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}