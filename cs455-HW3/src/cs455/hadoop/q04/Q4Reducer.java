package cs455.hadoop.q04;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.hadoop.Util.Artist;
import cs455.hadoop.Util.Song;

public class Q4Reducer extends Reducer<Text, Text, Text, Text>  {

	private double maxTotalTimeFading = -1;
	private String maxFadingArtistName = "";
	private ArrayList<Song> songs = new ArrayList<Song>();
	private HashMap<Artist, ArrayList<Song>> artists = new HashMap<Artist, ArrayList<Song>>();
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String songTitle = "";
        String artistID = "";
        String artistName = "";
        double totalFadeTime = 0;
        double duration = 0;
        double fadeInDuration = 0;
        double startOfFadeOut = 0;
        
        for (Text val : values) {
        	String[] record = val.toString().split(",");
        	for (int i = 0; i < record.length; i++) {
        		String parts[] = record[i].split("_");
        		if (parts[0].equals("ARTISTID")) {
        			artistID = parts[1];
        		} else if (parts[0].equals("ARTISTNAME")) {
        			artistName = parts[1];
        		} else if (parts[0].equals("SONGTITLE")) {
        			songTitle = parts[1];
        		} else if (parts[0].equals("DURATION")) {
        			duration = parseDouble(parts[1]);
        		} else if (parts[0].equals("ENDOFFADEIN")) {
        			fadeInDuration = parseDouble(parts[1]);
        		} else if (parts[0].equals("STARTOFFADEOUT")) {
        			startOfFadeOut = parseDouble(parts[1]);
        		}
        	}
        }
        
		Song song = new Song(key.toString(), songTitle);
		Artist artist = new Artist(artistID, artistName);
		
		double fadeOutDuration = duration - startOfFadeOut;
		totalFadeTime = fadeInDuration + fadeOutDuration;
		song.incrementTotalFadeTime(totalFadeTime);
		
		if (artists.containsKey(artist)) {
			ArrayList<Song> ss = artists.get(artist);
			ss.add(song);
			artists.put(artist, ss);
		} else {
			ArrayList<Song> ss = new ArrayList<Song>();
			ss.add(song);
			artists.put(artist, ss);
		}
		
		/**
        float totalFadeTime = 0;
        float duration = 0;
        float fadeInDuration = 0;
        float startOfFadeOut = 0;
        String artistName = "";
        
        for (Text val : values) {
        	String parts[] = val.toString().split(",");
        	artistName = parts[0];
        	duration = Float.parseFloat(parts[1]);
        	fadeInDuration = Float.parseFloat(parts[2]);
        	startOfFadeOut = Float.parseFloat(parts[3]);
        }
        
        float fadeOutDuration = duration - startOfFadeOut;
        totalFadeTime = fadeInDuration + fadeOutDuration;
        
        if (totalFadeTime > maxTotalTimeFading) {
        	maxTotalTimeFading = totalFadeTime;
        	maxFadingArtistName = artistName;
        }
        **/
    }
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text("Going to find the total time fading: "), new Text("We have this many artists " + artists.size()));
		
		double fadingTotal = 0.0;
        int numberOfLouds = 0;
        
        ArrayList<Artist> artistsList = new ArrayList<Artist>();
        
        
        for (Map.Entry<Artist, ArrayList<Song>> entry : artists.entrySet()) {
        	Artist a = entry.getKey();
        	ArrayList<Song> ss = entry.getValue();
        	for (Song s : entry.getValue()) {
        		a.incrementTotalTimeFading(s.getTotalFadeTime());
        	}
        	artistsList.add(a);
        	artists.put(a, ss);
        }
        
        artistsList.sort(Comparator.comparingDouble(Artist::getTotalTimeFading));
		Collections.reverse(artistsList);
        
		for (int i=0; i < 3; i++) {
			context.write(new Text(artistsList.get(i).getArtistName()), new Text("Really loves to fade this much: " + artistsList.get(i).getTotalTimeFading()));
		}
	}
	
	public static double parseDouble(String str) {
	    try {
	      return Double.parseDouble( str );
	    } catch ( NumberFormatException e ) {
	      return 0;
	    }
	 }
}
