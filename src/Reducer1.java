import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text,Text,NullWritable,Text>{
	
	/*
	 * Input: (state,years,necessary fields separated by commas)
	 * Output: (state,years,necessary fields separated by commas)
	 */
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws
	IOException, InterruptedException {

		for (Text value : values) {
			context.write(NullWritable.get(), new Text(key.toString() + value.toString()));
		}	
	}
}