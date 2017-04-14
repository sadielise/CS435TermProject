import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text>{
	
	/*
	 * Input: (dummy key, all fields separated by commas)
	 * Output: (state,years,maleOnly,femaleOnly, fieldsOfStudy, other necessary fields separated by commas including state)
	 */

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		int institutionIndex = 0;
		int institutionNameIndex = 1;
		int stateIndex = 5;
		int menIndex = 33;
		int womenIndex = 34;
		int firstDegreeIndex = 61;
		int lastDegreeIndex = 98;
		int firstIncomeIndex = 320;
		int lastIncomeIndex = 339;
		int meanEarningsIndex = 1629;
		int yearIndex = 1729;
		
		
		//split line
		String[] values = value.toString().split(",");

		List<String> toWrite = new ArrayList<String>();
		
		////KEY
		//state new index: 0
		toWrite.add(values[stateIndex]);
		//years new index: 1
		toWrite.add(values[yearIndex]);
		
		////VALUE
		//menOnly new index: 2
		toWrite.add(values[menIndex]);
		//womenOnly new index: 3
		toWrite.add(values[womenIndex]);
		//meanEarnings new index: 4
		toWrite.add(values[meanEarningsIndex]);
		//institutionID new index: 5
		toWrite.add(values[institutionIndex]);
		//institutionName new index: 6
		toWrite.add(values[institutionNameIndex]);
		//fieldsOfStudy new index: 7-44
		for(int i = firstDegreeIndex; i<=lastDegreeIndex; i++){
			toWrite.add(values[i]);
		}
		//income new index: 45-64
		for(int i = firstIncomeIndex; i<=lastIncomeIndex; i++){
			toWrite.add(values[i]);
		}
		
		String newKey = toWrite.get(0) + "," + toWrite.get(1) + ",";
		String newVal = toWrite.get(2);
		for(int i=3; i<toWrite.size(); i++){
			newVal+=newVal+","+toWrite.get(i);
		}
		
		//new size SHOULD == 65;
		
		if(toWrite.size() == 65){
			for(int i=0; i< toWrite.size(); i++){
				context.write(new Text(newKey), new Text(newVal));
			}
		}
	}
}