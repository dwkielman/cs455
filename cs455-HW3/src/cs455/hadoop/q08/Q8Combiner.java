package cs455.hadoop.q08;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.hadoop.Util.DataUtilities;

public class Q8Combiner extends Reducer<Text, Text, Text, Text>  {
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String artistID = "";
        String artistName = "";
		String songTitle = "";
		String info = "";
		String songHotttnesss = "";
		String timeSignature = "";
		String artistFamiliarity = "";
		String artistHotttnesss = "";
		String similarArtists = "";
        ArrayList<String> infoList = new ArrayList<String>();
        
        for (Text val : values) {
        	String parts[] = val.toString().split("\t");
        	if (parts[0].equals("analysis")) {
        		String[] record = parts[1].split(",");
        		songHotttnesss = record[0];
        		timeSignature = record[1];
        		infoList.add("SONGHOTTTNESSS_" + songHotttnesss);
        		infoList.add("TIMESIGNATURE_" + timeSignature);
        	} else if (parts[0].equals("metadata")) {
        		String[] record = parts[1].split(",");
        		artistID = record[0];
        		artistName = record[1];
        		songTitle = record[2];
        		artistFamiliarity = record[3];
        		artistHotttnesss = record[4];
        		similarArtists = record[5];
        		int similarArtistsSize = DataUtilities.dataReader(similarArtists).size();
        		infoList.add("ARTISTID_" + artistID);
        		infoList.add("ARTISTNAME_" + artistName);
        		infoList.add("SONGTITLE_" + songTitle);
        		infoList.add("ARTISTFAMILIARITY_" + artistFamiliarity);
        		infoList.add("ARTISTHOTTTNESSS_" + artistHotttnesss);
        		infoList.add("SIMILARARTISTS_" + String.valueOf(similarArtistsSize));
        	}
        }

        info = String.join(",", infoList);
		//infoToWrite.set(infoList.toString());
    	context.write(key, new Text(info));
		
		/**
		String songHotttnesss = "";
		String timeSignature = "";
		String artistID = "";
		String artistName = "";
		String artistFamiliarity = "";
		String artistHotttnesss = "";
		String songTitle = "";
		String similarArtists = "";
		
        for (Text val : values) {
        	String parts[] = val.toString().split("\t");
        	if (parts[0].equals("analysis")) {
        		String records[] = parts[1].split(",");
        		songHotttnesss = records[0];
        		timeSignature = records[1];
        	} else if (parts[0].equals("metadata")) {
        		String records[] = parts[1].split(",");
        		artistID = records[0];
        		artistName = records[1];
        		songTitle = records[2];
        		artistFamiliarity = records[3];
        		artistHotttnesss = records[4];
        		similarArtists = records[5];
        	}
        }
        
        context.write(key, new Text(songHotttnesss + "," + timeSignature + "," + songTitle + "," + artistID + "," + artistName + "," + artistFamiliarity + "," + artistHotttnesss + "," + similarArtists));
        **/
    }

}
