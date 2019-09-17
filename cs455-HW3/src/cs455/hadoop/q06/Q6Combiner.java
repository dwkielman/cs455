package cs455.hadoop.q06;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q6Combiner extends Reducer<Text, Text, Text, Text>  {

	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String artistID = "";
        String artistName = "";
        String info = "";
		String songTitle = "";
		String danceability = "";
		String energy = "";
        ArrayList<String> infoList = new ArrayList<String>();
        
        for (Text val : values) {
        	String parts[] = val.toString().split("\t");
        	if (parts[0].equals("analysis")) {
        		String[] record = parts[1].split(",");
        		danceability = record[0];
        		energy = record[1];
        		infoList.add("DANCEABILITY_" + danceability);
        		infoList.add("ENERGY_" + energy);
        	} else if (parts[0].equals("metadata")) {
        		String[] record = parts[1].split(",");
        		artistID = record[0];
        		artistName = record[1];
        		songTitle = record[2];
        		infoList.add("ARTISTID_" + artistID);
        		infoList.add("ARTISTNAME_" + artistName);
        		infoList.add("SONGTITLE_" + songTitle);
        	}
        }

        info = String.join(",", infoList);
		//infoToWrite.set(infoList.toString());
    	context.write(key, new Text(info));
    	
    	
		/**
		String danceability = "";
		String energy = "";
		String songTitle = "";
        
        for (Text val : values) {
        	String parts[] = val.toString().split("\t");
        	if (parts[0].equals("analysis")) {
        		String records[] = parts[1].split(",");
        		danceability = records[0];
        		energy = records[1];
        	} else if (parts[0].equals("metadata")) {
        		songTitle = parts[1];
        	}
        }
        
        context.write(key, new Text(danceability + "," + energy + "," + songTitle));
        **/
    }

}
