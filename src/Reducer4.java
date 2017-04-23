import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reducer4 extends Reducer<Text,Text,NullWritable,Text>{
	
	/*
	 * Input: ??
	 * Output: ??
	 */
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws
	IOException, InterruptedException {
		List<String> brackets = new ArrayList<String>();
		List<String> slopes = new ArrayList<String>();
		List<String> intercepts = new ArrayList<String>();
		List<String> RMSEs = new ArrayList<String>();
		String meanEarnings = null;
		String other = null;
		
		boolean si = false;
		boolean me = false;
		boolean cd = false;
		
		int maxYear = -1;
		
		for(Text val: values){
			String[] vals = val.toString().split(",");

			
			//SlopeIntercept
			if(vals.length == 5){
				si = true;
				brackets.add(new String(vals[1]));
				slopes.add(new String(vals[2]));
				intercepts.add(new String(vals[3]));
				RMSEs.add(new String(vals[4]));
			}
			
			//MeanEarnings
			else if(vals.length == 2){
				me = true;
				meanEarnings = new String(vals[1]);
			}
			
			//CleanData
			else{
				cd = true;
				int year = Integer.parseInt(vals[0]);
				if(year>maxYear){
					maxYear = year;
					other = new String(val.toString());
				}
			}
		}
		
		if(si && me && cd){
			for(int i=0; i<brackets.size(); i++){
				context.write(NullWritable.get(),new Text(other + "," + brackets.get(i) + "," + slopes.get(i) + "," + intercepts.get(i) + "," + RMSEs.get(i) + "," + meanEarnings));
			}
		}

		
	}
}