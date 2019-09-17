package cs455.hadoop.q06;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.hadoop.Util.Artist;
import cs455.hadoop.Util.DataUtilities;
import cs455.hadoop.Util.Song;

public class Q6Reducer extends Reducer<Text, Text, Text, Text>  {

	//private ArrayList<Song> songsList = new ArrayList<Song>();
	private ArrayList<Song> songs = new ArrayList<Song>();
	//private final int NUMBER_OF_SONGS = 10;
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String songTitle = "";
        String artistID = "";
        String artistName = "";
        double danceability = 0;
        double energy = 0;
        
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
        		} else if (parts[0].equals("DANCEABILITY")) {
        			danceability = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("ENERGY")) {
        			energy = DataUtilities.doubleReader(parts[1]);
        		}
        	}
        }
        
		Song song = new Song(key.toString(), songTitle);
		Artist artist = new Artist(artistID, artistName);
		
		song.setEnergyDouble(energy);
		song.setDanceabilityDouble(danceability);
		songs.add(song);
		
		/**
        float danceability = 0;
        float energy = 0;
        String songTitle = "";
        
        for (Text val : values) {
        	String parts[] = val.toString().split(",");
        	danceability = Float.parseFloat(parts[0]);
        	energy = Float.parseFloat(parts[1]);
        	songTitle = parts[2];
        	Song song = new Song(key.toString(), songTitle);
        	song.setDanceability(danceability);
        	song.setEnergy(energy);
        	songsList.add(song);
        }
        **/
    }
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		//songsList.stream().sorted(Comparator.comparing(Song::getDanceability).thenComparing(Song::getEnergy));
		songs.stream().sorted(Comparator.comparing(Song::getDanceabilityDouble).thenComparing(Song::getEnergyDouble));
		Collections.reverse(songs);
		
		ArrayList<Song> danceableSongs = new ArrayList<Song>();
		
		for (Song s : songs) {
			if (s.getDanceabilityDouble() > 0 && s.getEnergyDouble() > 0) {
				danceableSongs.add(s);
			}
		}
		
		int counter = 10;
		
		context.write(new Text("Most Danceable and Energetic Songs: "), new Text("We have this many songs that are the highly danceable and energetic " + danceableSongs.size()));
		
		if (danceableSongs.size() > 0) {
			if (danceableSongs.size() < 10) {
				counter = danceableSongs.size();
			}
			for (int i=0; i < counter; i++) {
				context.write(new Text(danceableSongs.get(i).getSongTitle()), new Text("Danceability: " + danceableSongs.get(i).getDanceabilityDouble() + ", Energy: " + danceableSongs.get(i).getEnergyDouble()));
			}
		} else {
			context.write(new Text("No Danceable or Energetic Songs Found."), new Text("Due to there being " + danceableSongs.size() + " number of Danceable Songs"));
		}
		
		/**
		for (int i = 0; i < NUMBER_OF_SONGS; i++) {
			Song song = songsList.get(i);
			context.write(new Text(song.getSongTitle()), new Text("Danceability: " + String.valueOf(song.getDanceability() + "\t Energy: " + String.valueOf(song.getEnergy()))));
		}
		**/
	}

}
