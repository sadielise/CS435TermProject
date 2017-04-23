import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MainClass4 {

	public static void main(String[] args) throws IOException, ClassNotFoundException,
	InterruptedException {
		
		if (args.length != 3) {
			System.out.printf("Usage: <jar file> <input dir>\n");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		
		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(MainClass4.class);
		job1.setJobName("merge files");
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setMapperClass(Mapper4.class);
		job1.setReducerClass(Reducer4.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileInputFormat.addInputPath(job1, new Path(args[1]));
		FileInputFormat.addInputPath(job1, new Path(args[2]));
		FileOutputFormat.setOutputPath(job1, new Path("/Job4"));
		
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
}
