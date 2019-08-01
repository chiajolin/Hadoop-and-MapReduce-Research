import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
//import static java.lang.Integer.*;


public class MyPartitioner extends Partitioner<MyWritable, NullWritable> {
  @Override
  public int getPartition(MyWritable myWritable, NullWritable value, int numPartitions) {
    return (myWritable.getPlayer().hashCode() & Integer.MAX_VALUE) % numPartitions;
  }
}