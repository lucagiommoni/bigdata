package it.polito.bigdata.hadoop.ex28;

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

		int numOfReducers	= 1;
		Path outputdir		= new Path(args[1]);
		Path inputPath1		= new Path(args[2]);
		Path inputPath2		= new Path(args[3]);

		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf);
		
		job.setJobName("Exercise 28");
		job.setJarByClass(MyDriver.class);

		// Set the path of the input file/folder
		MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, MyMapper1.class);
	    MultipleInputs.addInputPath(job, inputPath2, TextInputFormat.class, MyMapper2.class);		

		// Set the path of the output folder
		FileOutputFormat.setOutputPath(job, outputdir);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		// Set the mapper output key and value classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Set number of reducer
		job.setNumReduceTasks(numOfReducers);

		// Set reducer class
		if (numOfReducers != 0) {
		  job.setReducerClass(MyReducer.class);
		}

		// Execute the job and wait for completion
		if (!job.waitForCompletion(true))
			return 1;

		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new MyDriver(), args));
	}

}
