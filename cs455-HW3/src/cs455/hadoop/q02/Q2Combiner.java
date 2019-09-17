package cs455.hadoop.q02;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Q2Combiner extends Reducer<Text, Text, Text, Text>  {

	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String loudness = "";
        String artistID = "";
        String artistName = "";
        String info = "";
        ArrayList<String> infoList = new ArrayList<String>();
        
        for (Text val : values) {
        	String parts[] = val.toString().split("\t");
        	if (parts[0].equals("analysis")) {

        		loudness = parts[1];
        		infoList.add("LOUDNESS_" + loudness);
        		
        	} else if (parts[0].equals("metadata")) {
        		
        		String[] record = parts[1].split(",");
        		artistID = record[0];
        		artistName = record[1];
        		
        		infoList.add("ARTISTID_" + artistID);
        		infoList.add("ARTISTNAME_" + artistName);
        	}
        }

        info = String.join(",", infoList);
    	context.write(key, new Text(info));
    }
}
