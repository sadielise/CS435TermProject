import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper3 extends Mapper<LongWritable, Text, Text, Text>{
	
	/*
	 * Input: (datayear, necessary fields separated by commas)
	 * Output: (OPEID,bracket, datayear, annual cost) 
	 */

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		String[] vals = value.toString().split(",");
		String OPEID = vals[5];
		String year = vals[0];
		
		// get first income bracket
		int bracketIndex1 = 45;
		String key1 = OPEID + ",1";
		String value1 = year + "," + vals[bracketIndex1];
		context.write(new Text(key1), new Text(value1));
		
		// get second income bracket
		int bracketIndex2 = 46;
		String key2 = OPEID + ",2";
		String value2 = year + "," + vals[bracketIndex2];
		context.write(new Text(key2), new Text(value2));
		
		// get third income bracket
		int bracketIndex3 = 47;
		String key3 = OPEID + ",3";
		String value3 = year + "," + vals[bracketIndex3];
		context.write(new Text(key3), new Text(value3));
		
		// get fourth income bracket
		int bracketIndex4 = 48;
		String key4 = OPEID + ",4";
		String value4 = year + "," + vals[bracketIndex4];
		context.write(new Text(key4), new Text(value4));
		
		// get fifth income bracket
		int bracketIndex5 = 49;
		String key5 = OPEID + ",5";
		String value5 = year + "," + vals[bracketIndex5];
		context.write(new Text(key5), new Text(value5));
		
		
	}
}