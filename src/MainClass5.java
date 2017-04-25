import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MainClass5 {

	public static void main(String[] args) throws IOException, ClassNotFoundException,
	InterruptedException {
		
		if (args.length != 2) {
			System.out.printf("Usage: <database> <query>\n");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		
		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(MainClass5.class);
		job1.setJobName("user query");
		
		try {
			job1.addCacheFile(new URI(args[1]+"#query"));
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setMapperClass(Mapper5.class);
		job1.setReducerClass(Reducer5.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("/Job5"));
		
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
}
