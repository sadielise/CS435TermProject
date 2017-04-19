import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<Text,Text,NullWritable,Text>{
	
	/*
	 * Input: (OPEID, earnings)
	 * Output: (OPEID, average earnings)
	 */
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws
	IOException, InterruptedException {
	
		int count = 0;
		int total = 0;
		for(Text val: values){
			String strVal = val.toString();
			int intVal = Integer.valueOf(strVal);
			total += intVal;
			count++;
		}
		
		int average = total / count;
		String strAverage = String.valueOf(average);
		
		context.write(NullWritable.get(), new Text(key.toString() + "," + strAverage));
	}
}