package com.tailtarget.bigdata.examples.crunch;

import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.tailtarget.bigdata.examples.hadoop.Event;

public class GetTimeAndSourceBucket extends MapFn<Event, Pair<Long, String>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Pair<Long, String> map(Event event) {
		long minuteBucket =
				new DateTime(event.getTimestamp()).withZone(DateTimeZone.UTC).minuteOfDay().roundFloorCopy().getMillis();
		return Pair.of(minuteBucket, event.getSource());
	}
	
}
