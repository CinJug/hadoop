package com.tailtarget.bigdata.examples.crunch;

import java.util.List;

import org.apache.avro.generic.GenericData;
import org.apache.crunch.PCollection;
import org.apache.crunch.io.ReadableSource;
import org.apache.crunch.io.avro.AvroFileSource;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.util.CrunchTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;
import org.kitesdk.data.crunch.CrunchDatasets;
import org.kitesdk.data.filesystem.impl.Accessor;

import com.google.common.collect.Lists;
import com.tailtarget.bigdata.examples.hadoop.Event;
import com.tailtarget.bigdata.examples.hadoop.Summary;

public class GenerateSummaries extends CrunchTool {

	@Override
	public int run(String[] args) throws Exception {
		DatasetRepository repo = DatasetRepositories.open("repo:file:/data/sand_box");
		Dataset<Event> eventsDataset = repo.load("events");
		Dataset<Summary> summariesDataset = repo.load("summaries");

		PCollection<Event> events = read(asSource(eventsDataset, Event.class));

		PCollection<Summary> summaries =
				events.by(new GetTimeAndSourceBucket(), Avros.pairs(Avros.longs(), Avros.strings())).groupByKey()
						.parallelDo(new MakeSummary(), Avros.reflects(Summary.class));

		write(summaries, CrunchDatasets.asTarget(summariesDataset));
		
		run();
		done();

		return 0;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int rc = ToolRunner.run(conf, new GenerateSummaries(), args);
		System.exit(rc);
	}

	public static <E> ReadableSource<E> asSource(Dataset<E> dataset, Class<E> type) {
		Path directory = Accessor.getDefault().getDirectory(dataset);
		if (directory != null) {
			List<Path> paths = Lists.newArrayList(Accessor.getDefault().getPathIterator(dataset));

			AvroType<E> avroType;
			if (type.isAssignableFrom(GenericData.Record.class)) {
				avroType = (AvroType<E>) Avros.generics(dataset.getDescriptor().getSchema());
			} else {
				avroType = Avros.records(type);
			}
			final Format format = dataset.getDescriptor().getFormat();
			if (Formats.PARQUET.equals(format)) {
				return null;
			} else if (Formats.AVRO.equals(format)) {
				return new AvroFileSource<E>(paths.get(0), avroType);
			} else {
				throw new UnsupportedOperationException("Not a supported format: " + format);
			}
		}
		return null;
	}
}
