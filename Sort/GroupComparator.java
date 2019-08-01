import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {
	public GroupComparator() {
    	super(MyWritable.class, true);
    }

    @Override
    public int compare(WritableComparable p1, WritableComparable p2) {
    	MyWritable pair1 = (MyWritable) p1;
        MyWritable pair2 = (MyWritable) p2;
        return pair1.getPlayer().compareTo(pair2.getPlayer());
    }
}