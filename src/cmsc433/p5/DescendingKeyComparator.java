package cmsc433.p5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DescendingKeyComparator extends WritableComparator {

	protected DescendingKeyComparator() {
		super(IntWritable.class, true);
		// TODO Auto-generated constructor stub
	}
	
	 @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	        IntWritable key1 = (IntWritable) w1;
	        IntWritable key2 = (IntWritable) w2;          
	        return -1 * key1.compareTo(key2);
	    }

}
