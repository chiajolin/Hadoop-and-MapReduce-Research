import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyComparator extends WritableComparator {
	public MyComparator() {
    	super(MyWritable.class, true);
    }

    @Override
    public int compare(WritableComparable p1, WritableComparable p2) {
    	MyWritable pair1 = (MyWritable) p1;
        MyWritable pair2 = (MyWritable) p2;
        return pair1.compareTo(pair2);
    }
}