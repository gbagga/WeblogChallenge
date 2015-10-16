package com.paytmlabs.challenge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.paytmlabs.challenge.customwritables.CompositeKey;
import com.paytmlabs.challenge.customwritables.CompositeValue;
import com.paytmlabs.challenge.customwritables.ReducerCompositeValue;
import com.paytmlabs.challenge.customwritables.ReducerSessionInfoWritable;

public class WeblogReducer extends Reducer<CompositeKey,CompositeValue,Text,ReducerCompositeValue>{

	ReducerCompositeValue reducerCompositeValue = new ReducerCompositeValue();
	ReducerSessionInfoWritable sessionInfo = new ReducerSessionInfoWritable();
	int sessionOutTime = 0;
	@SuppressWarnings("rawtypes")
	private MultipleOutputs mos = null;
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
    public void setup(Context context) {
        mos = new MultipleOutputs(context);
        Configuration config = context.getConfiguration();
        String sessionTimeOut = config.get("session.timeout.mins");
        sessionOutTime = Integer.valueOf(sessionTimeOut);
    }

	@SuppressWarnings("unchecked")
	public void reduce(CompositeKey key,Iterable<CompositeValue> values,Context context) throws IOException,InterruptedException
	{
		long previousTimeStamp = 0L;
		long currentTimeStamp = 0L;
		int sessionNum = 0;
		long currentSessionTimeStamp = 0L;
		ArrayList<Long> sessionsTimes = new ArrayList<Long>();
		Set<String> urls = new HashSet<String>();
		String url = null;
		String session_id = null;
		boolean firstSession = true;
		
		for(CompositeValue value:values)
		{
		
			currentTimeStamp =value.getTimestamp().get();
			url = value.getUrl().toString();
			reducerCompositeValue.setTimeStamp(value.getTimeStampString());
			reducerCompositeValue.setUrl(value.getUrl());
			
			if(!firstSession && getDifference(currentTimeStamp,previousTimeStamp) < sessionOutTime*60 )
			{
				reducerCompositeValue.setSession_id(new Text(session_id));
				reducerCompositeValue.setSession_no(new IntWritable(sessionNum));
				mos.write(key.getIp(),reducerCompositeValue,"sessionized_data/part" );
				urls.add(url);
				previousTimeStamp = currentTimeStamp;
				
			}else
			{
				
				
				//New Session
				//Calculate things for previous Session 
				if(!firstSession)
				{
					long sessionTime = getDifference(previousTimeStamp,currentSessionTimeStamp);
					sessionsTimes.add(sessionTime);
					int uniqueURLs = urls.size();
					
					
					//Collect the data sessionTime , unique url count
					sessionInfo.setSessionNum(new IntWritable(sessionNum));
					sessionInfo.setSessionTime(new Text(String.valueOf(sessionTime)));
					sessionInfo.setUrlNum(new IntWritable(uniqueURLs));
					mos.write(key.getIp(),sessionInfo,"session_info/part" );
				}
				if(firstSession)
				{
					firstSession = false;
				}
				
				
				//Set sessionStart
				currentSessionTimeStamp = currentTimeStamp;
				sessionNum++;
				session_id = "Session-"+currentSessionTimeStamp;
				urls = new HashSet<String>();
				urls.add(url);
				
				reducerCompositeValue.setSession_id(new Text(session_id));
				reducerCompositeValue.setSession_no(new IntWritable(sessionNum));
				mos.write(key.getIp(),reducerCompositeValue,"sessionized_data/part" );
				previousTimeStamp = currentTimeStamp;
			}
			
				
		}
		
		long sessionTime = getDifference(previousTimeStamp,currentSessionTimeStamp);
		
		sessionsTimes.add(sessionTime);
		sessionInfo.setSessionNum(new IntWritable(sessionNum));
		sessionInfo.setSessionTime(new Text(String.valueOf(sessionTime)));
		sessionInfo.setUrlNum(new IntWritable(urls.size()));
		mos.write(key.getIp(),sessionInfo,"session_info/part" );
		
		
		float avgSessionSize = 0F;
		for(Long f:sessionsTimes)
			avgSessionSize+=f;
		
		Float avgSize = (float)avgSessionSize/sessionsTimes.size();
		
		mos.write(key.getIp(),new FloatWritable(avgSize),"avg_session/part" );
	}

	@Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
	
	private long getDifference(Long time1,Long time2)
	{
		return time1-time2;
	}

	
}
