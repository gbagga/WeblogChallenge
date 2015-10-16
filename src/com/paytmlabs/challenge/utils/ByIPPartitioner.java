package com.paytmlabs.challenge.utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import com.paytmlabs.challenge.customwritables.CompositeKey;
import com.paytmlabs.challenge.customwritables.CompositeValue;

public class ByIPPartitioner extends Partitioner<CompositeKey,CompositeValue>{

	HashPartitioner<Text,CompositeValue> partitioner = new HashPartitioner<Text,CompositeValue>();
	Text ipKey = new Text();
	
	@Override
	public int getPartition(CompositeKey key, CompositeValue value, int numPartitions) {
		// TODO Auto-generated method stub
		
		try{
			ipKey.set(key.getIp());
			return partitioner.getPartition(ipKey, value, numPartitions);
		}catch(Exception e)
		{
			e.printStackTrace();
			return (int) (Math.random() * numPartitions);
		}
		
	}

}
