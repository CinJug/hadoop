package com.tailtarget.bigdata.examples.crunch;

import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.DatasetRepository;

import com.tailtarget.bigdata.examples.hadoop.Summary;

public class ReadSummaries {

	public static void main(String[] args) throws Exception {
		DatasetRepository repo = DatasetRepositories.open("repo:file:/data/sand_box");
		Dataset<Summary> summaries = repo.load("summaries");
		DatasetReader<Summary> reader = summaries.newReader();
		reader.open();
		try {
			for (Summary summary: reader) {
				System.out.println(summary);
			}
		} finally {
			reader.close();
		}
	}
	
}
