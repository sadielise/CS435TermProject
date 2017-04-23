import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper4 extends Mapper<LongWritable, Text, Text, Text>{
	
	/*
	 * Input: 3 files (SlopeIntercept, MeanEarnings, CleanData)
	 * Output: ??
	 */

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String[] vals = value.toString().split(",");
		
		//SlopeIntercept
		if(vals.length == 5){
			context.write(new Text(vals[0]), value);
		}
		
		//MeanEarnings
		else if(vals.length == 2){
			context.write(new Text(vals[0]), value);
		}
		
		//CleanData
		else if(vals.length > 5){
			context.write(new Text(vals[5]), value);
		}
	}
}