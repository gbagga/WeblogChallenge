package com.paytmlabs.challenge;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.paytmlabs.challenge.customwritables.CompositeKey;
import com.paytmlabs.challenge.customwritables.CompositeValue;
import com.paytmlabs.challenge.utils.ByIPGroupingComparator;
import com.paytmlabs.challenge.utils.ByIPPartitioner;
import com.paytmlabs.challenge.utils.ByIPTimeStampSortComparator;

public class WeblogJob extends Configured  implements Tool{

	public int run(String[] args) throws Exception
	{
		
		Job job = Job.getInstance(getConf(), "Weblog Challenge");
		job.setJarByClass(WeblogJob.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(WeblogMapper.class);
		job.setReducerClass(WeblogReducer.class);
		
		job.setMapOutputKeyClass(CompositeKey.class);
		job.setMapOutputValueClass(CompositeValue.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setPartitionerClass(ByIPPartitioner.class);
		job.setSortComparatorClass(ByIPTimeStampSortComparator.class);
		job.setGroupingComparatorClass(ByIPGroupingComparator.class);
		
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		return job.waitForCompletion(true)?0:1;
		
		
	}
	
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode = ToolRunner.run(new WeblogJob(),args);
		System.exit(exitCode);
		
	}

}
