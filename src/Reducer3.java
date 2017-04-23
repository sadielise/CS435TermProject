import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer3 extends Reducer<Text,Text,NullWritable,Text>{
	
	/*
	 * Input: (OPEID,bracket_index, datayear,annual cost) 
	 * Output: (OPEID,bracket_index,slope,intercept, RMSE)
	 */
	
	List<Double> years;
	List<Double> costs;
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws
	IOException, InterruptedException {
		
		SimpleRegression sr = new SimpleRegression(true);
		
		years = new ArrayList<Double>();
		costs = new ArrayList<Double>();
		
		int count = 0;
		for(Text val: values){
			count++;
			String[] vals = val.toString().split(",");
			double year = Double.valueOf(vals[0]);
			double cost = Double.valueOf(vals[1]);
			sr.addData(year, cost);
			years.add(year);
			costs.add(cost);
		}
		
		
		double slope = sr.getSlope();
		double intercept = sr.getIntercept();
		
		 
		
		if(count > 1){
			double error = this.getRMSE();
			context.write(NullWritable.get(), new Text(key.toString() + "," + String.valueOf(slope) + "," + String.valueOf(intercept) + "," + String.valueOf(error)));
		}
	}
	
	double getRMSE(){
		if(years.size()<3){
			return -1;
		}
		
		double sumErr = 0;
		for(int i=0; i< years.size(); i++){
			sumErr += getSquaredErrForIndex(i);
		}
		
		return Math.sqrt(sumErr/years.size());
	}
	
	double getSquaredErrForIndex(int index){
		SimpleRegression sr = new SimpleRegression(true);
		
		for(int i=0; i<years.size(); i++){
			if(i!=index){
				sr.addData(years.get(i), costs.get(i));
			}
		}
		
		double estimate = sr.getIntercept() + sr.getSlope()*years.get(index);
		return Math.pow(estimate - this.costs.get(index), 2);
	}
}