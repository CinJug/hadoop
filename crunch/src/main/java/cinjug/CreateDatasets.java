package com.tailtarget.bigdata.examples.hadoop;

import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.DatasetRepository;

public class CreateDatasets {

	public static void main(String[] args) throws Exception {
		DatasetRepository repo = DatasetRepositories.open("repo:file:/data/sand_box");

		DatasetDescriptor summaryDescriptor = new DatasetDescriptor.Builder().schema(Summary.class).build();
		repo.create("summaries", summaryDescriptor);
		
		DatasetDescriptor descriptor = new DatasetDescriptor.Builder().schema(Event.class).build();
		repo.create("events", descriptor);
	}

}
