package cs455.hadoop.q02;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.hadoop.Util.Song;

public class Q2Reducer extends Reducer<Text, Text, Text, Text>  {

	private ArrayList<String> artists = new ArrayList<String>();
	private HashMap<String, ArrayList<Song>> artistMap = new HashMap<String,  ArrayList<Song>>();
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        Song s = new Song();
        
        String artistID = "";
        String artistName = "";
        double loudness = 0.0;
        
        for (Text val : values) {
        	String[] record = val.toString().split(",");
        	for (int i = 0; i < record.length; i++) {
        		String parts[] = record[i].split("_");
        		if (parts[0].equals("ARTISTID")) {
        			artistID = parts[1];
        		} else if (parts[0].equals("ARTISTNAME")) {
        			artistName = parts[1];
        		} else if (parts[0].equals("LOUDNESS")) {
        			loudness = parseDouble(parts[1]);
        		}
        	}
        }
        
        s.setArtistID(artistID);
        s.setArtistName(artistName);
        s.setLoudnessDouble(loudness);
        
        if (artistMap.containsKey(artistID)) {
        	ArrayList<Song> ss = new ArrayList<Song>();
        	ss = artistMap.get(artistID);
        	ss.add(s);
        	artistMap.put(artistID, ss);
        } else {
        	ArrayList<Song> ss = new ArrayList<Song>();
        	ss.add(s);
        	artistMap.put(artistID, ss);
        }
        
        if (!artists.contains(artistID)) {
        	artists.add(artistID);
        }
    }
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
        double totalLoudness = 0.0;
        int numberOfLouds = 0;
        
		for (String a : artists) {
			ArrayList<Song> songs = new ArrayList<Song>();
			songs = (artistMap.get(a));
			
			for (Song s : songs) {
				if (s.getLoudnessDouble() < 0.0) {
					totalLoudness += s.getLoudnessDouble();
					numberOfLouds++;
				}
			}
		}
		
		double loudnessAverage = totalLoudness / numberOfLouds;
		
		double theLoudestAverage = 0.0;
		String theLoudestArtistName = "";
		
		for (String a : artists) {
			ArrayList<Song> songs = new ArrayList<Song>();
			songs = (artistMap.get(a));
			String artistName = "";
			
			for (Song s : songs) {
				totalLoudness = 0.0;
		        numberOfLouds = 0;;
				if (s.getLoudnessDouble() < 0.0) {
					totalLoudness += s.getLoudnessDouble();
					numberOfLouds++;
					artistName = s.getArtistName();
				}
			}
			
			double artistLoudnessAverage = totalLoudness / numberOfLouds;
			
			if (artistLoudnessAverage > loudnessAverage) {
				theLoudestAverage = artistLoudnessAverage;
				theLoudestArtistName = artistName;
			}
		}
		
		context.write(new Text(theLoudestArtistName), new Text("Average Highest Loudness: " + theLoudestAverage));
	}
	
	public static double parseDouble(String str) {
	    try {
	      return Double.parseDouble( str );
	    } catch ( NumberFormatException e ) {
	      return 0;
	    }
	 }
}
