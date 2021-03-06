package it.polito.bigdata.hadoop.ex27;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
 * Apr 24, 2018
 */
public class MyDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		Path inputPath		= new Path(args[2]);
		Path outputdir		= new Path(args[1]);

		Configuration conf = this.getConf();
		
		// Define a new job
		Job job = Job.getInstance(conf);

		// Assign a name to the job
		job.setJobName("Exercise 27");
		
		// Specify the class of the driver
		job.setJarByClass(MyDriver.class);
		
		// Add HDFS file stopwords.txt in the distributed cache
	    // stopwords.txt can now be read by all mappers of this application
	    // independently of the nodes of the cluster used to run the application
		job.addCacheFile(new Path("cache/dictionary.txt").toUri());

		// Set the path of the input file/folder
		FileInputFormat.addInputPath(job, inputPath);

		// Set the path of the output folder
		FileOutputFormat.setOutputPath(job, outputdir);

		// Set the job input format
		job.setInputFormatClass(TextInputFormat.class);
		
		// Set the job output format
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set the mapper class
		job.setMapperClass(MyMapper.class);

		// Set the mapper output key and value classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		// Set number of reducer
		job.setNumReduceTasks(0);
		

		// Execute the job and wait for completion
		if (!job.waitForCompletion(true))
			return 1;	

		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new MyDriver(), args));
	}
}