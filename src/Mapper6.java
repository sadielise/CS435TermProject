import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper6 extends Mapper<LongWritable, Text, Text, Text>{
	
	/*
	 * Input: ??
	 * Output: ??
	 */

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

	}
}