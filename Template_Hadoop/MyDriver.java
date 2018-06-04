package it.polito.bigdata.hadoop.ex;

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

	public static enum MY_COUNTERS {}

	@Override
	public int run(String[] args) throws Exception {

		int numOfReducers	= Integer.parseInt(args[0]);
		Path outputdir		= new Path(args[1]);
		Path inputPath1		= new Path(args[2]);
		// Path inputPath2		= new Path(args[3]);

		Configuration conf = this.getConf();

		// Set configuration parameter
		conf.set("name", "value");

		// Define a new job
		Job job = Job.getInstance(conf);

		// Assign a name to the job
		job.setJobName("Exercise xx");

		// Specify the class of the driver
		job.setJarByClass(MyDriver.class);

		// Set the path of the input file/folder
		FileInputFormat.addInputPath(job, inputPath1);

		// Set the path of the output folder
		FileOutputFormat.setOutputPath(job, outputdir);

		// Set multiple input files
		// MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, MyMapper1.class);
		// MultipleInputs.addInputPath(job, inputPath2, TextInputFormat.class, MyMapper2.class);

		// Set the job input format
		job.setInputFormatClass(TextInputFormat.class);

		// Set the job output format
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set reducer output key and value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// Set the mapper class
		job.setMapperClass(MyMapper.class);

		// Set the mapper output key and value classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		// Set number of reducer
		job.setNumReduceTasks(numOfReducers);

		// Set reducer class
		if (numOfReducers != 0) {
		  job.setReducerClass(MyReducer.class);
		}

		// Set combiner class
		job.setCombinerClass(MyCombiner.class);

		// Execute the job and wait for completion
		if (!job.waitForCompletion(true))
			return 1;

		for (MY_COUNTERS c : MY_COUNTERS.values()) {
			System.out.println(c.name() + " = " + job.getCounters().findCounter(c).getValue());
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new MyDriver(), args));
	}

}
