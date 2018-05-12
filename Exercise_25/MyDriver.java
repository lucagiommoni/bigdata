package it.polito.bigdata.hadoop.ex25;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * MyDriver.java
 *
 * @version 1.0
 *
 */
public class MyDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Path outputdir		= new Path(args[1]);
		Path tmpOutputdir		= new Path("tmp25");
		Path inputPath1		= new Path(args[2]);

		Configuration conf1 = this.getConf();

		// Define a new job
		Job job1 = Job.getInstance(conf1);

		// Assign a name to the job
		job1.setJobName("Exercise 25 - Job 1");

		// Specify the class of the driver
		job1.setJarByClass(MyDriver.class);

		// Set the path of the input file/folder
		FileInputFormat.addInputPath(job1, inputPath1);
		
		// Set the path of the output folder
		FileOutputFormat.setOutputPath(job1, tmpOutputdir);

		// Set the job input format
		job1.setInputFormatClass(TextInputFormat.class);
		
		// Set the job output format
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		// Set reducer output key and value classes
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		// Set the mapper class
		job1.setMapperClass(MyMapper1.class);

		// Set the mapper output key and value classes
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		// Set number of reducer
		job1.setNumReduceTasks(1);
		
		// Set reducer class
		job1.setReducerClass(MyReducer.class);

		// Execute the job and wait for completion
		if (!job1.waitForCompletion(true))
			return 1;

		Configuration conf2 = this.getConf();

		// Define a new job
		Job job2 = Job.getInstance(conf2);

		// Assign a name to the job
		job2.setJobName("Exercise 25 - Job 2");

		// Specify the class of the driver
		job2.setJarByClass(MyDriver.class);

		// Set the path of the input file/folder
		FileInputFormat.addInputPath(job2, tmpOutputdir);
		
		// Set the path of the output folder
		FileOutputFormat.setOutputPath(job2, outputdir);

		// Set the job input format
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		
		// Set the job output format
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		// Set reducer output key and value classes
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		// Set the mapper class
		job2.setMapperClass(MyMapper2.class);

		// Set the mapper output key and value classes
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		// Set number of reducer
		job2.setNumReduceTasks(1);
		
		// Set reducer class
		job2.setReducerClass(MyReducer.class);

		// Execute the job and wait for completion
		if (!job2.waitForCompletion(true))
			return 1;
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new MyDriver(), args));
	}

}
