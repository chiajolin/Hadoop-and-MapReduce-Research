import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

public class MyWritable implements Writable, WritableComparable<MyWritable> {
  private Text player = new Text();
  private IntWritable totalGame = new IntWritable();
  private IntWritable distinctOpponent = new IntWritable();

  public MyWritable(){
    this.player = new Text();
    this.totalGame = new IntWritable();
    this.distinctOpponent = new IntWritable();
  }
  public MyWritable(Text player, IntWritable totalGame,IntWritable distinctOpponent){
    this.player = player;
    this.totalGame = totalGame;
    this.distinctOpponent = distinctOpponent;
  }

  public void set(Text player, IntWritable totalGame, IntWritable distinctOpponent){
    this.player = player;
    this.totalGame = totalGame;
    this.distinctOpponent = distinctOpponent;
  }

  public Text getPlayer(){
    return player;
  }

  public IntWritable getTotalGame(){
    return totalGame;
  }

  public IntWritable getDistinctOpponent(){
    return distinctOpponent;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    player.readFields(in);
    totalGame.readFields(in);
    distinctOpponent.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    player.write(out);
    totalGame.write(out);
    distinctOpponent.write(out);
  }

  @Override
  public String toString() {
      return player.toString() + "\t" + totalGame.toString() + "\t" + distinctOpponent.toString();
  }

  @Override
  public int compareTo(MyWritable pair) {
    int compareValue = this.totalGame.compareTo(pair.getTotalGame());
    if (compareValue == 0) {
      compareValue = distinctOpponent.compareTo(pair.getDistinctOpponent());
    }
    return -1*compareValue;
  }
}