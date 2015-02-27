package org.arpit.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.IOException;


public class GetStationReducer extends Reducer<Text, NullWritable, Text, Text> {

    private GSODStationParser metadata;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        metadata = new GSODStationParser();
        metadata.initialize(new File("isd-history.txt"));
    }

    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        String stationName = metadata.getStationName(key.toString());
        String ID = key.toString();
        context.write(new Text(stationName), new Text(ID));
    }
}
