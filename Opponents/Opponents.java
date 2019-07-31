import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
arg[0] = input
arg[1] = output
*/
public class Opponents {
  public static class CustomWritable implements Writable{
    private IntWritable gameNumber, conWin, conLose, conWinTag, conLoseTag;
    public CustomWritable(){
      this.gameNumber = new IntWritable();
      this.conWin = new IntWritable();
      this.conLose = new IntWritable();
      this.conWinTag = new IntWritable();
      this.conLoseTag = new IntWritable();
    }
    public CustomWritable(IntWritable gameNumber,IntWritable conWin,IntWritable conLose, IntWritable conWinTag,IntWritable conLoseTag){
      this.gameNumber = gameNumber;
      this.conWin = conWin;
      this.conLose = conLose;
      this.conWinTag = conWinTag;
      this.conLoseTag = conLoseTag;
    }
    public void set(IntWritable gameNumber,IntWritable conWin,IntWritable conLose, IntWritable conWinTag,IntWritable conLoseTag){
      this.gameNumber = gameNumber;
      this.conWin = conWin;
      this.conLose = conLose;
      this.conWinTag = conWinTag;
      this.conLoseTag = conLoseTag;
    }
    public IntWritable getGameNumber(){
      return gameNumber;
    }
    public IntWritable getConWin(){
      return conWin;
    }
    public IntWritable getConLose(){
      return conLose;
    }
    public IntWritable getConWinTag(){
      return conWinTag;
    }
    public IntWritable getConLoseTag(){
      return conLoseTag;
    }
    @Override
    public void readFields(DataInput in) throws IOException {
      gameNumber.readFields(in);
      conWin.readFields(in);
      conLose.readFields(in);
      conWinTag.readFields(in);
      conLoseTag.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      gameNumber.write(out);
      conWin.write(out);
      conLose.write(out);
      conWinTag.write(out);
      conLoseTag.write(out);
    }

    @Override
    public String toString() {
        return gameNumber.toString() + "\t" + conWin.toString() + "\t" + conLose.toString() + "\t" + conWinTag.toString() + "\t" + conLoseTag.toString();
    }
  }

  public static class OutputWritable implements Writable{
    private IntWritable gameNumber;
    private Text conWin, conLose;
    public OutputWritable(){
      this.gameNumber = new IntWritable();
      this.conWin = new Text();
      this.conLose = new Text();
    }
    public OutputWritable(IntWritable gameNumber,Text conWin,Text conLose){
      this.gameNumber = gameNumber;
      this.conWin = conWin;
      this.conLose = conLose;
    }
    public void set(IntWritable gameNumber,Text conWin,Text conLose){
      this.gameNumber = gameNumber;
      this.conWin = conWin;
      this.conLose = conLose;
    }
    @Override
    public void readFields(DataInput in) throws IOException {
      gameNumber.readFields(in);
      conWin.readFields(in);
      conLose.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      gameNumber.write(out);
      conWin.write(out);
      conLose.write(out);
    }

    @Override
    public String toString() {
        return gameNumber.toString() + "\t" + conWin.toString() + "\t" + conLose.toString();
    }
  }

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, CustomWritable>{
    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable zero = new IntWritable(0);
    private final static IntWritable na = new IntWritable(-1); //tag, if != -1 => not na
    private Text textKey = new Text();
    private CustomWritable valueMap = new CustomWritable();

    private String tagToken;
    private String keyContent;
    private String playerToken;
    private String color;
    private String whitePlayer;
    private String blackPlayer;

    private String whiteEloToken;
    private int whiteEloScore;
    private String blackEloToken;
    private int blackEloScore;

    private String resultToken;
    private String result;

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        tagToken = itr.nextToken();
 
        if(tagToken.equals("[White")){
          playerToken = itr.nextToken();
          color = tagToken.substring(1,tagToken.length());
          whitePlayer = playerToken.substring(1,playerToken.length()-2);
        } 

        if(tagToken.equals("[Black")){
          playerToken = itr.nextToken();
          color = tagToken.substring(1,tagToken.length());
          blackPlayer = playerToken.substring(1,playerToken.length()-2);
        }

        if(tagToken.equals("[WhiteElo")){
          whiteEloToken = itr.nextToken();
          whiteEloScore = Integer.parseInt(whiteEloToken.substring(1,whiteEloToken.length()-2));
        }

        if(tagToken.equals("[BlackElo")){
          blackEloToken = itr.nextToken();
          blackEloScore = Integer.parseInt(blackEloToken.substring(1,blackEloToken.length()-2));
        }

        if(tagToken.equals("[Result")){
          //na or not
          if(blackEloScore > whiteEloScore){
            keyContent = blackPlayer + "\t" + "Black";
            textKey.set(keyContent);
            valueMap.set(zero, zero, zero, na, one);
            context.write(textKey, valueMap);

            keyContent = whitePlayer + "\t" + "White";
            textKey.set(keyContent);
            valueMap.set(zero, zero, zero, one, na);
            context.write(textKey, valueMap);
          }
          else if(whiteEloScore > blackEloScore){
            keyContent = blackPlayer + "\t" + "Black";
            textKey.set(keyContent);
            valueMap.set(zero, zero, zero, one, na);
            context.write(textKey, valueMap);

            keyContent = whitePlayer + "\t" + "White";
            textKey.set(keyContent);
            valueMap.set(zero, zero, zero, na, one);
            context.write(textKey, valueMap);
          }

          //write result
          resultToken = itr.nextToken();
          result = resultToken.substring(1,resultToken.length()-2);
          if(result.equals("1/2-1/2")){ //black and white 都要設value = (1,0,0)
            keyContent = blackPlayer + "\t" + "Black";
            textKey.set(keyContent);
            valueMap.set(one, zero, zero, na, na);
            context.write(textKey, valueMap);

            keyContent = whitePlayer + "\t" + "White";
            textKey.set(keyContent);
            valueMap.set(one, zero, zero, na, na);
            context.write(textKey, valueMap);
          }

        //white win, black lose
          else if(result.equals("1-0")){ 
            if(blackEloScore > whiteEloScore){ //white 設 value = (1,1,0), black 設 value = (1,0,1)
              keyContent = whitePlayer + "\t" + "White";
              textKey.set(keyContent);
              valueMap.set(one, one, zero, na, na);
              context.write(textKey, valueMap);

              keyContent = blackPlayer + "\t" + "Black";
              textKey.set(keyContent);
              valueMap.set(one, zero, one, na, na);
              context.write(textKey, valueMap);
            }
            else{
              keyContent = whitePlayer + "\t" + "White";
              textKey.set(keyContent);
              valueMap.set(one, zero, zero, na, na);
              context.write(textKey, valueMap);

              keyContent = blackPlayer + "\t" + "Black";
              textKey.set(keyContent);
              valueMap.set(one, zero, zero, na, na);
              context.write(textKey, valueMap);
            }
          }

        //black win, white lose
          else if(result.equals("0-1")){
            if(whiteEloScore > blackEloScore){ //black 設 value = (1,1,0), white 設 value = (1,0,1)
              keyContent = whitePlayer + "\t" + "White";
              textKey.set(keyContent);
              valueMap.set(one, zero, one, na, na);
              context.write(textKey, valueMap);

              keyContent = blackPlayer + "\t" + "Black";
              textKey.set(keyContent);
              valueMap.set(one, one, zero, na, na);
              context.write(textKey, valueMap);
            }
            else{
              keyContent = whitePlayer + "\t" + "White";
              textKey.set(keyContent);
              valueMap.set(one, zero, zero, na, na);
              context.write(textKey, valueMap);

              keyContent = blackPlayer + "\t" + "Black";
              textKey.set(keyContent);
              valueMap.set(one, zero, zero, na, na);
              context.write(textKey, valueMap);
            }
          }
        }
      }
    }
  }

    public static class MyCombiner
       extends Reducer<Text,CustomWritable,Text,CustomWritable> {
    private CustomWritable result = new CustomWritable();
    //private OutputWritable result = new OutputWritable();
    private Text winPercentage = new Text();
    private Text losePercentage = new Text();

    public void reduce(Text key, Iterable<CustomWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int gamesum = 0;
      int winsum = 0;
      int losesum = 0;
      int conWinTag = -1; //na
      int conLoseTag = -1; //na
      //String winsumString;
      //String losesumString;
      for (CustomWritable val : values) {
        //sum += val.get();
        gamesum += val.getGameNumber().get();
        winsum += val.getConWin().get();
        losesum += val.getConLose().get();
        if(val.getConWinTag().get() == 1){
          conWinTag = 1; //not na
        }
        if(val.getConLoseTag().get() == 1){
          conLoseTag = 1; //not na
        }
      }

      //result.set(new IntWritable(gamesum),new IntWritable(winsum),new IntWritable(losesum), new IntWritable(conWinTag), new IntWritable(conLoseTag));
      result.set(new IntWritable(gamesum), new IntWritable(winsum), new IntWritable(losesum), new IntWritable(conWinTag), new IntWritable(conLoseTag));
      //result.set(new IntWritable(gamesum), winPercentage,losePercentage);
      context.write(key, result);
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,CustomWritable,Text,OutputWritable> {
    //private CustomWritable result = new CustomWritable();
    private OutputWritable result = new OutputWritable();
    private Text winPercentage = new Text();
    private Text losePercentage = new Text();

    public void reduce(Text key, Iterable<CustomWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int gamesum = 0;
      int winsum = 0;
      int losesum = 0;
      int conWinTag = -1; //na
      int conLoseTag = -1; //na
      //String winsumString;
      //String losesumString;
      for (CustomWritable val : values) {
        //sum += val.get();
        gamesum += val.getGameNumber().get();
        winsum += val.getConWin().get();
        losesum += val.getConLose().get();
        if(val.getConWinTag().get() == 1){
          conWinTag = 1; //not na
        }
        if(val.getConLoseTag().get() == 1){
          conLoseTag = 1; //not na
        }
      }

      if(conWinTag != 1){
        winPercentage.set("na");
      }
      else{
        winPercentage.set(Float.toString(((float)winsum/gamesum)));
      } 

      if(conLoseTag != 1){
        losePercentage.set("na");
      }
      else{
        losePercentage.set(Float.toString(((float)losesum/gamesum)));
      } 
      //result.set(new IntWritable(gamesum),new IntWritable(winsum),new IntWritable(losesum), new IntWritable(conWinTag), new IntWritable(conLoseTag));
      result.set(new IntWritable(gamesum), winPercentage,losePercentage);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Opponents");
    job.setJarByClass(Opponents.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(MyCombiner.class);
    job.setReducerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(CustomWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(OutputWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}