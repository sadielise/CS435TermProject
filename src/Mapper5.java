import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper5 extends Mapper<LongWritable, Text, DoubleWritable, Text>{
	
	/*
	 * user query: start_year,state/all,field_of_study,gender,family_income_bracket,years
	 * Output: ??
	 */
	
	//end fields from data
	//...,bracket,slope,intercept,RMSE,meanEarnings
	
	String[] query;
	//from query
	int startYearQuery = 0;
	int stateQuery = 1;
	int fieldQuery = 2;
	int genderQuery = 3;
	int bracketQuery = 4;
	int numYearsQuery = 5;
	
	//from data
	int stateData = 2;
	int startFieldData = 7;
	int menData = 3;
	int womenData = 4;
	int yearData = 2;
	
	int bracketData;
	int slopeData;
	int interceptData;
	int RMSEData;
	int meanEarningsData;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		this.getQuery();
		
		String[] vals = value.toString().split(",");
		
		int len = vals.length;
		bracketData = len-5;
		slopeData = len-4;
		interceptData = len-3;
		RMSEData = len-2;
		meanEarningsData = len-1;
		
		if(!this.filterData(vals)){
			return;
		}

		
		double costEstimate = this.getCostEstimate(vals);
		double totalError = this.getTotalError(vals);
		double satisfactionScore = this.getSatisfactionScore(vals, costEstimate);
		
		context.write(new DoubleWritable(satisfactionScore), new Text(costEstimate + "," + totalError + "," + value.toString()));
	}
	
	private double getSatisfactionScore(String[] vals, double cost) {
		double meanEarnings = Double.parseDouble(vals[this.meanEarningsData]);
		return meanEarnings/cost;
	}

	private double getTotalError(String[] vals) {
		int numYears = Integer.parseInt(query[this.numYearsQuery]);
		double RMSE = Double.parseDouble(vals[RMSEData]);
		return RMSE*numYears;
	}

	private double getCostEstimate(String[] vals) {
		double slope = Double.parseDouble(vals[slopeData]);
		double intercept = Double.parseDouble(vals[interceptData]);
		double startYear = Double.parseDouble(query[this.startYearQuery]);
		int numYears = Integer.parseInt(query[this.numYearsQuery]);
		
		double cost = 0;
		for(int i=0; i<numYears; i++){
			cost+=slope*(startYear+i) + intercept;
		}
		return cost;
	}

	private boolean filterData(String[] vals){
		//state
		if(!query[stateQuery].equalsIgnoreCase("all") && !query[stateQuery].equalsIgnoreCase(vals[stateData])){
			return false;
		}
		
		//income bracket
		if(Integer.parseInt(query[this.bracketQuery]) != Integer.parseInt(vals[this.bracketData])){
			return false;
		}
		
		//field
		if(Double.parseDouble(vals[startFieldData + fieldQuery])==0){
			return false;
		}
		
		//gender
		//no males in female only schools
		if(Integer.parseInt(vals[womenData])!=0){
			if(query[genderQuery].equalsIgnoreCase("m")){
				return false;  
			}
		}
		//no females in male only schools
		else if(Integer.parseInt(vals[menData])!=0){
			if(query[genderQuery].equalsIgnoreCase("f")){ 
				return false; 
			}
		}
		
		//number of years to attend
		int requestedYears = Integer.parseInt(query[numYearsQuery]);
		int dataYears = Integer.parseInt(vals[yearData]);
		if(dataYears == 1){
			dataYears = 4;
		}
		else if(dataYears == 3){
			dataYears = 1;
		}
		if(requestedYears != dataYears){
			return false;
		}
		
		return true;
	}
	
	private void getQuery() throws IOException{
		File file = new File("./query");
		FileReader fileReader = new FileReader(file);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String line;
		if ((line = bufferedReader.readLine()) != null) {
			this.query = line.toString().split(",");
		}
		bufferedReader.close();
	}
}