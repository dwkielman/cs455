package cs455.hadoop.q04;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q4Combiner extends Reducer<Text, Text, Text, Text>  {

	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String artistID = "";
        String artistName = "";
        String info = "";
        String duration = "";
		String endOfFadeIn = "";
		String startOfFadeOut = "";
		String songTitle = "";
        ArrayList<String> infoList = new ArrayList<String>();
        
        for (Text val : values) {
        	String parts[] = val.toString().split("\t");
        	if (parts[0].equals("analysis")) {
        		String[] record = parts[1].split(",");
        		duration = record[0];
        		endOfFadeIn = record[1];
        		startOfFadeOut = record[2];
        		infoList.add("DURATION_" + duration);
        		infoList.add("ENDOFFADEIN_" + endOfFadeIn);
        		infoList.add("STARTOFFADEOUT_" + startOfFadeOut);
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
		String artistName = "";
        
        for (Text val : values) {
        	String parts[] = val.toString().split("\t");
        	if (parts[0].equals("analysis")) {
        		String records[] = parts[1].split(",");
        		
        		duration = records[0];
        		endOfFadeIn = records[1];
        		startOfFadeOut = records[2];

        	} else if (parts[0].equals("metadata")) {
        		artistName = parts[1];
        	}
        }
        
        context.write(key, new Text(artistName + "," + duration + "," + endOfFadeIn + "," + startOfFadeOut));
        **/
    }

}
