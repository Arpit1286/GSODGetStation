package org.arpit.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.IOException;

/**
 * Created by Arpit on 2/25/2015.
 */
public class GetStationReducer extends Reducer<Text, NullWritable, Text, Text> {

    private GSODStationParser metadata;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        metadata = new GSODStationParser();
        metadata.initialize(new File("is-history.txt"));
    }

    public void redduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        String stationName = metadata.getStationName(key.toString());
        context.write(new Text(stationName), new );
    }
}