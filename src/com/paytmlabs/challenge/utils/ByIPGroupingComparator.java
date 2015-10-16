package com.paytmlabs.challenge.utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.paytmlabs.challenge.customwritables.CompositeKey;

public class ByIPGroupingComparator extends WritableComparator {

	protected ByIPGroupingComparator()
	{
		super(CompositeKey.class,true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1,WritableComparable w2)
	{
		CompositeKey ck1 = (CompositeKey) w1;
		CompositeKey ck2 = (CompositeKey) w2;
		return ck1.getIp().compareTo(ck2.getIp());
	}
	
}
