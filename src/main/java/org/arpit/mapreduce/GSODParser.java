package org.arpit.mapreduce;

import org.apache.hadoop.io.Text;

/**
 * Created by Arpit on 2/25/2015.
 */
public class GSODParser {
    private static final float MISSING_TEMPERATURE = (float) 9999.9;

    private String stationID;
    private String year;
    private float airTemperature;
    //private boolean airTemperaturemalformed;

    public void parse(String record) {
        year = record.substring(14,18);
        String airTemperatureString = record.substring(102,108);
        airTemperature = Float.parseFloat(airTemperatureString.trim());
        stationID = record.substring(0,6);
    }

    public void parse(Text record) {
        parse(record.toString());
    }

    public String getStationID() {
        return stationID;
    }

    public String getYear() {
        return year;
    }

    public float getTemperature(){
        return airTemperature;
    }
}
