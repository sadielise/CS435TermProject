import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper3 extends Mapper<LongWritable, Text, Text, Text>{
	
	/*
	 * Input: (university name, score, state, average net price, average earnings)
	 * Output: (university name, score, state, average net price, average earnings)
	 */

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

	}
}