package cs455.hadoop.q10;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q10Combiner extends Reducer<Text, Text, Text, Text>  {

	private final StringBuilder sb = new StringBuilder();
	private final Text output = new Text();
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		for (Text val : values) {
        	String parts[] = val.toString().split("\t");
        	if (parts[0].equals("analysis")) {
        		String[] record = parts[1].split(",");

        		sb.append("SONGHOTTTNESSS_#_" + record[0]);
        		sb.append(",");
        		output.set(sb.toString());
        		sb.setLength(0);
        		sb.trimToSize();
        		context.write(key, output);
        	} else if (parts[0].equals("metadata")) {
        		String[] record = parts[1].split(",");
        		sb.append("ARTISTID_#_" + record[0]);
        		sb.append(",");
        		sb.append("ARTISTNAME_#_" + record[1]);
        		sb.append(",");
        		sb.append("SONGTITLE_#_" + record[2]);
        		sb.append(",");
        		sb.append("ARTISTHOTTTNESSS_#_" + record[3]);
        		sb.append(",");
        		sb.append("LOCATION_#_" + record[4]);
        		sb.append(",");
        		sb.append("ARTISTYEAR_#_" + record[5]);
        		sb.append(",");
        		
        		output.set(sb.toString());
        		sb.setLength(0);
        		sb.trimToSize();
        		context.write(key, output);
        	}
        }
		
		
		/**
		String songHotttnesss = "";
		String songTitle = "";
		String artistHotttness = "";
		String artistLatitude = "";
		String artistLongitude = "";
		String city = "";
		String state = "";
		String country = "";
		String artistName = "";
		String year = "";

        for (Text val : values) {
        	String parts[] = val.toString().split("\t");
        	if (parts[0].equals("analysis")) {
        		songHotttnesss = parts[1];
        	} else if (parts[0].equals("metadata")) {
        		String records[] = parts[1].split(",");
        		songTitle = records[0];
        		artistHotttness = records[1];
        		artistLatitude = records[2];
        		artistLongitude = records[3];
        		String artistDetailsRecords[] = records[4].split("|");
        		city = artistDetailsRecords[0];
        		state = artistDetailsRecords[1];
        		country = artistDetailsRecords[2];
        		artistName = records[5];
        		year = records[6];
        	}
        }
        
        context.write(key, new Text(songHotttnesss + "," + songTitle + "," + artistHotttness + "," + artistLatitude + "," + artistLongitude + "," + city + "," + state + "," + country + "," + artistName + "," + year));
        **/
    }

}
