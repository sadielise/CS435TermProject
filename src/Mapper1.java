import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text>{
	
	/*
	 * Input: (dummy key, all fields separated by commas)
	 * Output: (state,years,maleOnly,femaleOnly, fieldsOfStudy, other necessary fields separated by commas including state)
	 */

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		int institutionIndex = 2; //OPEID 6-digit
		int institutionNameIndex = 3;
		int stateIndex = 5;
		int menIndex = 33;
		int womenIndex = 34;
		int firstDegreeIndex = 61;
		int lastDegreeIndex = 98;
		
		int firstCostNoIncomeIndex = 316;
		int lastCostNoIncomeIndex = 319;
		
		int firstIncomeIndex = 320;
		int lastIncomeIndex = 339;
		//int meanEarningsIndex = 1638;
		int yearIndex = 1738;
		
		//split line
		String[] values = value.toString().split(",");

		List<String> toWrite = new ArrayList<String>();
		
		////KEY
		//state new index: 0
		toWrite.add(new String(values[stateIndex]));
		//years new index: 1
		toWrite.add(new String(values[yearIndex]));
		
		////VALUE
		//menOnly new index: 2
		toWrite.add(new String(values[menIndex]));
		//womenOnly new index: 3
		toWrite.add(new String(values[womenIndex]));
		//meanEarnings new index: 4
		//toWrite.add(new String(values[meanEarningsIndex]));
		//institutionID new index: 5
		if(values[institutionIndex].equalsIgnoreCase("OPEID6")){
			return;
		}
		toWrite.add(new String(values[institutionIndex]));
		//institutionName new index: 6
		toWrite.add(new String(values[institutionNameIndex]));
		//fieldsOfStudy new index: 7-44
		for(int i = firstDegreeIndex; i<=lastDegreeIndex; i++){
			if(values[i].equalsIgnoreCase("NULL")){
				toWrite.add(new String("0"));
			}
			else{
				toWrite.add(new String(values[i]));
			}
		}
		//cost by income bracket new index: 45-64
		int count = 0;
		for(int i = firstIncomeIndex; i<=lastIncomeIndex; i++){
			if(!values[i].equalsIgnoreCase("NULL") && count<5){
				toWrite.add(new String(values[i]));
				count++;
			}
		}
		
		//cost no income if income values not available
		if(count == 0){
			for(int i = firstCostNoIncomeIndex; i<= lastCostNoIncomeIndex; i++){
				if(!values[i].equalsIgnoreCase("NULL")){
					toWrite.add(new String(values[i]));
					toWrite.add(new String(values[i]));
					toWrite.add(new String(values[i]));
					toWrite.add(new String(values[i]));
					toWrite.add(new String(values[i]));
					count++;
					break;
				}
			}
		}
		
		if(count!=0){
			String prev = new String(toWrite.get(toWrite.size()-1));
			while(count<5){
				toWrite.add(new String(prev));
				count++;
			}
			
			
			context.getInputSplit();
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName()+",";
			//String newKey = "2000,";context.getInputSplit();
			
					
					
			String newVal =	new String(toWrite.get(0));
			for(int i=1; i<toWrite.size(); i++){
				newVal = newVal+","+toWrite.get(i);
			}
			
			//new size SHOULD == 64;
			
			if(toWrite.size() != 0){
				context.write(new Text(fileName), new Text(newVal));
			}
		}
	}
}