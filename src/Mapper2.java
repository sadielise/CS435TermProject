import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<LongWritable, Text, Text, Text>{
	
	/*
	 * Input: (OPEID, earnings)
	 * Output: (OPEID, earnings)
	 */

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String[] vals = value.toString().split(",");
		String id = vals[0];
		String earnings = vals[1];
		
		context.write(new Text(id), new Text(earnings));
	}
}