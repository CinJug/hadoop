package com.tailtarget.bigdata.examples.crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import com.tailtarget.bigdata.examples.hadoop.Event;
import com.tailtarget.bigdata.examples.hadoop.Summary;

public class MakeSummary extends DoFn<Pair<Pair<Long, String>, Iterable<Event>>, Summary> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void process(Pair<Pair<Long, String>, Iterable<Event>> input, Emitter<Summary> emitter) {
		Summary summary = new Summary();
		summary.setBucket(input.first().first());
		summary.setSource(input.first().second());
		for (Event event: input.second()) {
			summary.incrementCount();
		}
		emitter.emit(summary);
	}
	
}
