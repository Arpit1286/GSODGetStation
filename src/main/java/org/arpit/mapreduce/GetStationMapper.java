package org.arpit.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Arpit on 2/25/2015.
 */
public class GetStationMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private GSODParser parser = new GSODParser();

    public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {

        parser.parse(value);
        context.write(new Text(parser.getStationID()), NullWritable.get());
    }
}
