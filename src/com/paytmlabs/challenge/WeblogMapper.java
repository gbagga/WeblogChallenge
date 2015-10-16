package com.paytmlabs.challenge;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.paytmlabs.challenge.customwritables.CompositeKey;
import com.paytmlabs.challenge.customwritables.CompositeValue;
import com.paytmlabs.challenge.utils.WeblogIPParser;
public class WeblogMapper extends Mapper<LongWritable,Text,CompositeKey,CompositeValue> {

	private WeblogIPParser weblogIPParser = new WeblogIPParser();
	enum Log{
		CORRUPT
	}
	
	public void map(LongWritable key,Text value,Context context) throws InterruptedException,IOException
	{
		weblogIPParser.parse(value);
		if(weblogIPParser.isValidWebLog())
		{
			context.write(new CompositeKey(weblogIPParser.getClientIp(),weblogIPParser.getTimeStamp()),
					new CompositeValue(weblogIPParser.getTimeStamp(),weblogIPParser.getUrl(),weblogIPParser.getTimeStampString()));
		}else
		{
			System.err.println("Ignoring corrupt log"+value);
			context.getCounter(Log.CORRUPT).increment(1);
		}
	}
}
