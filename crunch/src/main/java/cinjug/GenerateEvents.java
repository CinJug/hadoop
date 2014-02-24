package com.tailtarget.bigdata.examples.hadoop;

import java.util.UUID;

import org.apache.log4j.Logger;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.DatasetWriter;

public class GenerateEvents {

	public static void main(String[] args) throws Exception {
		DatasetRepository repo = DatasetRepositories.open("repo:file:/data/sand_box");
		Dataset<Event> eventsDataset = repo.load("events");
		DatasetWriter<Event> writer = eventsDataset.newWriter();
		writer.open();
		
		Logger logger = Logger.getLogger(GenerateEvents.class);
		long i = 0;
		String source = UUID.randomUUID().toString();
		while (i < 1000) {
			Event event = new Event();
			event.setId(i++);
			event.setTimestamp(System.currentTimeMillis());
			event.setSource(source);
			
			writer.write(event);
			logger.info(event);
			Thread.sleep(100);
		}
		
		writer.close();
	}

}
