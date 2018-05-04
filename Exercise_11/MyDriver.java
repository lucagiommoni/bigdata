package it.polito.bigdata.hadoop.ex11;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
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

		Path inputPath		= new Path(args[0]);
		Path outputdir		= new Path(args[1]);

		Configuration conf = this.getConf();

		// Define a new job
		Job job = Job.getInstance(conf);

		// Assign a name to the job
		job.setJobName("Exercise 11");

		// Set the path of the input file/folder
		FileInputFormat.addInputPath(job, inputPath);

		// Set the path of the output folder
		FileOutputFormat.setOutputPath(job, outputdir);

		// Specify the class of the driver
		job.setJarByClass(MyDriver.class);

		// Set the job input and  output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set the Mapper class
		job.setMapperClass(MyMapper.class);

		// Set the mapper output key and value classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MyCustom.class);

		// Set number of reducer
		job.setNumReduceTasks(1);
		
		// Set the Reducer class
		job.setReducerClass(MyReducer.class);

		// Set reducer output key and value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// Execute the job and wait for completion
		if (!job.waitForCompletion(true))
			return 1;

		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new MyDriver(), args));
	}

}
