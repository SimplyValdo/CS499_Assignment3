/*Ubaldo Jimenez Prieto
 * February 28, 2016
 * Assignment # 3
 * CS499
 */

package com.cs499.AS3;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopCountDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TopCountDriver(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception 
	{
		if (args.length != 8) 
		{
			System.err.printf("Usage: %s needs three arguments, input1, input2, output1 and output2 files\n",
			getClass().getSimpleName());
			return -1;
		}
		
		FileUtils.deleteDirectory(new File(args[2]));
		FileUtils.deleteDirectory(new File(args[3]));
		FileUtils.deleteDirectory(new File(args[4]));
		FileUtils.deleteDirectory(new File(args[4]));
		FileUtils.deleteDirectory(new File(args[5]));
		FileUtils.deleteDirectory(new File("FinalResults"));

		//----------------------------------------------------------------------------------------------------//	
		
		Job job = new Job();
		job.setJarByClass(TopCountDriver.class);
		job.setJobName("Top10Movies");

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
	
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(Top10MoviesMapClass.class);
		job.setReducerClass(Top10MoviesReduceClass.class);

		int returnValue = job.waitForCompletion(true) ? 0 : 1;

		if (job.isSuccessful()) {
			System.out.println("Job 1 was successful");
		} else if (!job.isSuccessful()) {
			System.out.println("Job was not successful");
		}
				
		//----------------------------------------------------------------------------------------------------//

		Job job2 = new Job();
		job2.setJarByClass(TopCountDriver.class);
		job2.setJobName("Top10Users");

		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));

		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		

		job2.setMapperClass(Top10UsersMapClass.class);
		job2.setReducerClass(Top10UsersReduceClass.class);

		int returnValue2 = job2.waitForCompletion(true) ? 0 : 1;

		if (job2.isSuccessful()) {
			System.out.println("Job 2 was successful");
		} else if (!job2.isSuccessful()) {
			System.out.println("Job 2 was not successful");
		}

		System.out.println(returnValue);
		System.out.println(job2.isSuccessful());
		
		//----------------------------------------------------------------------------------------------------//
		
		Job job3 = new Job();
		job3.setJarByClass(TopCountDriver.class);
		job3.setJobName("SortTop10Users");

		FileInputFormat.addInputPath(job3, new Path(args[2] + "/part-r-00000"));
		FileOutputFormat.setOutputPath(job3, new Path(args[4]));
		
		job3.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		job3.setOutputKeyClass(DoubleWritable.class);
		job3.setOutputValueClass(IntWritable.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		

		job3.setMapperClass(Top10MoviesMapSortClass.class);

		int returnValue3 = job3.waitForCompletion(true) ? 0 : 1;

		if (job3.isSuccessful()) {
			System.out.println("Job 3 was successful");
		} else if (!job3.isSuccessful()) {
			System.out.println("Job 3 was not successful");
		}

		System.out.println(returnValue);
		System.out.println(job3.isSuccessful()); 
		
		//----------------------------------------------------------------------------------------------------//
		
		Job job4 = new Job();
		job4.setJarByClass(TopCountDriver.class);
		job4.setJobName("SortTop10Users");

		FileInputFormat.addInputPath(job4, new Path(args[3] + "/part-r-00000"));
		FileOutputFormat.setOutputPath(job4, new Path(args[5]));

		job4.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		job4.setOutputKeyClass(DoubleWritable.class);
		job4.setOutputValueClass(IntWritable.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		

		job4.setMapperClass(Top10MoviesMapSortClass.class);

		int returnValue4 = job4.waitForCompletion(true) ? 0 : 1;

		if (job4.isSuccessful()) {
			System.out.println("Job 4 was successful");
		} else if (!job4.isSuccessful()) {
			System.out.println("Job 4 was not successful");
		}

		System.out.println(returnValue);
		System.out.println(job4.isSuccessful()); 
		System.out.println();
		
		//----------------------------------------------------------------------------------------------------//
		
		findTopMoviesAndUsers printResults = new findTopMoviesAndUsers(args[0], args[4], args[5], args[6], args[7]);
		printResults.storemoviesList();
		printResults.findTop10Movies();
		printResults.findTopTenUsers();

		return returnValue;
	}
}