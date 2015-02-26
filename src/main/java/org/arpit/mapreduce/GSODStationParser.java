package org.arpit.mapreduce;

import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.util.*;


/**
 * Created by Arpit on 2/25/2015.
 */
public class GSODStationParser {
    private Map<String, String> stationIdToName = new HashMap<String, String>();

    public void initialize(File file) throws IOException {
        BufferedReader in = null;
        try {
            in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            StationMetaDataParser parser = new StationMetaDataParser();
            String line;
            while ((line = in.readLine()) != null) {
                if (parser.parse(line)) {
                    stationIdToName.put(parser.getStationID(), parser.getStationName());
                }
            }
        } finally {
            IOUtils.closeStream(in);
        }
    }

    public String getStationName(String stationId) {
        String stationName = stationIdToName.get(stationId);
        if (stationName == null || stationName.trim().length() == 0) {
            return stationId;  // return ID in case of no mathc
        }
        return stationName;
    }

    public Map<String, String> getStationIdToNameMap() {
        return Collections.unmodifiableMap(stationIdToName);
    }
}
