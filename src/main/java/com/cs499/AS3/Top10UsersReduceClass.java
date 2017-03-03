/*Ubaldo Jimenez Prieto
 * February 28, 2016
 * Assignment # 3
 * CS499
 */

package com.cs499.AS3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Top10UsersReduceClass extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> 
{

	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values,
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException 
	{

		int numOfReviews = 0;
		Iterator<IntWritable> i = values.iterator();
		
		while(i.hasNext()) 
		{
			int temp = i.next().get();
			numOfReviews += temp;
		}
		
		context.write(key, new IntWritable(numOfReviews));
	}

}