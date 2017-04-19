import java.io.IOException;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer3 extends Reducer<Text,Text,NullWritable,Text>{
	
	/*
	 * Input: (OPEID,bracket_index, datayear,annual cost) 
	 * Output: (OPEID,bracket_index,slope,intercept)
	 */
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws
	IOException, InterruptedException {
		
		SimpleRegression sr = new SimpleRegression(true);
		
		for(Text val: values){
			String[] vals = val.toString().split(",");
			double year = Double.valueOf(vals[0]);
			double cost = Double.valueOf(vals[1]);
			sr.addData(year, cost);
		}
		
		double slope = sr.getSlope();
		double intercept = sr.getIntercept();
		
		context.write(NullWritable.get(), new Text(key.toString() + "," + String.valueOf(slope) + "," + String.valueOf(intercept)));
		
	}
}