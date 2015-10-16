package com.paytmlabs.challenge.utils;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

@Description(name = "iso8601_to_unix_timestamp",
value = "_FUNC_(string) - convert an ISO 8601 datetime string to a UNIX timestamp",
extended = "Example:\n"
         + "  > SELECT * FROM src WHERE _FUNC_(date_string) > 1370991663;")
public class ISODateConverter extends UDF {

	public ISODateConverter() {

    }

    public long evaluate(String iso8601Date) {
        DateTime dateTime = new DateTime(iso8601Date, DateTimeZone.UTC);

        return dateTime.getMillis() / 1000;
    }
	
}
