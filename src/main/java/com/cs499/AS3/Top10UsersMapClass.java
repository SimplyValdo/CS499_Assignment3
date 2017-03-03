/*Ubaldo Jimenez Prieto
 * February 28, 2016
 * Assignment # 3
 * CS499
 */

package com.cs499.AS3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Top10UsersMapClass extends Mapper<LongWritable, Text, IntWritable, IntWritable> 
{

	private IntWritable userID = new IntWritable();
	private IntWritable incrementTotalReview = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException 
	{

		String[] line = value.toString().split(",");
		userID.set(Integer.parseInt(line[1]));
		context.write(userID, incrementTotalReview);
		System.out.println(userID +"-" + incrementTotalReview);
		
		}
}