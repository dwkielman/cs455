package cs455.hadoop.q03;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.hadoop.Util.Song;

public class Q3Reducer extends Reducer<Text, Text, Text, Text>  {

	private double maxSongHotttnesss = -1.0;
	private ArrayList<Song> songs = new ArrayList<Song>();
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double songHotttnesss = -2.0;
        String songTitle = "";
        String artistID = "";
        String artistName = "";
        
        for (Text val : values) {
        	String[] record = val.toString().split(",");
        	for (int i = 0; i < record.length; i++) {
        		String parts[] = record[i].split("_");
        		if (parts[0].equals("ARTISTID")) {
        			artistID = parts[1];
        		} else if (parts[0].equals("ARTISTNAME")) {
        			artistName = parts[1];
        		} else if (parts[0].equals("SONGHOTTTNESSS")) {
        			songHotttnesss = parseDouble(parts[1]);
        			//loudness = Double.parseDouble(parts[1]);
        		} else if (parts[0].equals("SONGTITLE")) {
        			songTitle = parts[1];
        		}
        	}
        	//String parts[] = val.toString().split(",");
        	//songHotttnesss = Float.parseFloat(parts[0]);
        	//songTitle = parts[1];
        }
        
		Song song = new Song(key.toString(), songTitle);
		song.setArtistID(artistID);
		song.setArtistName(artistName);
		song.setSongHotttnesssDouble(songHotttnesss);
		songs.add(song);
        
        if (songHotttnesss > maxSongHotttnesss) {
        	maxSongHotttnesss = songHotttnesss;
        }
    }
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text("The highest Song hotttnesss is: "), new Text("This number: " + maxSongHotttnesss));
		ArrayList<Song> hottestSongs = new ArrayList<Song>();
		
		for (Song s : songs) {
			if (s.getSongHotttnesssDouble() == maxSongHotttnesss) {
				hottestSongs.add(s);
			}
		}
		
		songs.sort(Comparator.comparingDouble(Song::getSongHotttnesssDouble));
		Collections.reverse(songs);
		
		for (int i=0; i < 3; i++) {
			context.write(new Text(songs.get(i).getSongTitle()), new Text("Song is so hot it's a: " + songs.get(i).getSongHotttnesssDouble()));
		}
		
		/**
		for (Song s : hottestSongs) {
			context.write(new Text(s.getSongTitle()), new Text("Song is so hot it's a: " + s.getSongHotttnesssDouble()));
		}
		**/
		
	}
	
	public static double parseDouble(String str) {
	    try {
	      return Double.parseDouble( str );
	    } catch ( NumberFormatException e ) {
	      return 0;
	    }
	 }

}
