package cs455.hadoop.q03;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q3Combiner extends Reducer<Text, Text, Text, Text>  {

	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String artistID = "";
        String artistName = "";
        String info = "";
        String songHotttnesss = "";
		String songTitle = "";
        ArrayList<String> infoList = new ArrayList<String>();
        
        for (Text val : values) {
        	String parts[] = val.toString().split("\t");
        	if (parts[0].equals("analysis")) {
        		songHotttnesss = parts[1];
        		infoList.add("SONGHOTTTNESSS_" + songHotttnesss);
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
        
        for (Text val : values) {
        	String parts[] = val.toString().split("\t");
        	if (parts[0].equals("analysis")) {
        		songHotttnesss = parts[1];
        	} else if (parts[0].equals("metadata")) {
        		songInfo = parts[1];
        	}
        }
        
        context.write(key, new Text(songHotttnesss + "," + songInfo));
        **/
    }

}
