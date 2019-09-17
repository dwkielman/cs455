package cs455.hadoop.q10;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.hadoop.Util.Artist;
import cs455.hadoop.Util.DataUtilities;
import cs455.hadoop.Util.Song;

public class Q10Reducer extends Reducer<Text, Text, Text, Text>  {

	private ArrayList<Song> songs = new ArrayList<Song>();
	private List<Integer> years = new ArrayList<Integer>();
	private final int SONGS_PER_YEAR = 10;
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String songTitle = "";
        String artistID = "";
        String artistName = "";
        String location = "";
        double songHotttnesss = 0.0;
        double artistHotttnesss = 0.0;
		int year = 0;
        
        for (Text val : values) {
        	String[] record = val.toString().split(",");
        	for (int i = 0; i < record.length; i++) {
        		String parts[] = record[i].split("_#_");
        		if (parts[0].equals("ARTISTID")) {
        			artistID = parts[1];
        		} else if (parts[0].equals("ARTISTNAME")) {
        			artistName = parts[1];
        		} else if (parts[0].equals("SONGTITLE")) {
        			songTitle = parts[1];
        		} else if (parts[0].equals("SONGHOTTTNESSS")) {
        			songHotttnesss = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("ARTISTHOTTTNESSS")) {
        			artistHotttnesss = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("LOCATION")) {
        			location = parts[1];
        		} else if (parts[0].equals("ARTISTYEAR")) {
        			year = (int) DataUtilities.doubleReader(parts[1]);
        			if (!years.contains(year) && year != 0) {
                		years.add(year);
                	}
        		} 
        	}
        }
        
        // null locations are useless
        if (!location.equals("b''")) {
        	Song song = new Song(key.toString(), songTitle);
    		Artist artist = new Artist(artistID, artistName);
    		song.setSongHotttnesssDouble(songHotttnesss);
    		song.setArtistHotttness(artistHotttnesss);
    		artist.setArtistHotttnesss(artistHotttnesss);
    		song.setArtistID(artistID);
    		song.setArtistName(artistName);
    		song.setCityDetails(location);
    		song.setYear(year);
    		songs.add(song);
        }
        
		
		/**
		float songHotttnesss = 0;
		String songTitle = "";
		float artistHotttness = 0;
		double artistLatitude = 0.0;
		double artistLongitude = 0.0;
		String city = "";
		String state = "";
		String country = "";
		String artistName = "";
		int year = 0;

        for (Text val : values) {
        	String parts[] = val.toString().split(",");
        	songHotttnesss = Float.parseFloat(parts[0]);
        	songTitle = parts[1];
        	artistHotttness = Float.parseFloat(parts[2]);
        	artistLatitude = Double.parseDouble(parts[3]);
        	artistLongitude = Double.parseDouble(parts[4]);
        	city = parts[5];
        	state = parts[6];
        	songTitle = parts[7];
        	artistName = parts[8];
        	year = Integer.parseInt(parts[9]);
        	if (!years.contains(year) && year != 0) {
        		years.add(year);
        	}
        }
        
		Song song = new Song(key.toString(), songTitle);
		song.setSongHotttnesss(songHotttnesss);
		song.setArtistHotttness(artistHotttness);
		song.setLatitude(artistLatitude);
		song.setLongitude(artistLongitude);
		song.setCityDetails("City: " + city + ", State: " + state + ", Country: " + country);
		song.setArtistName(artistName);
		song.setYear(year);
		songs.add(song);
		**/
    }
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		ArrayList<Song> hottestSongsPerYear = new ArrayList<Song>();
		
		songs.sort(Comparator.comparingInt(Song::getYear));	
		Collections.reverse(songs);
		
		years.sort(Comparator.naturalOrder());
		Collections.reverse(years);
		//Collections.sort(songs, Collections.reverseOrder());
		
		//Collections.sort(years, Collections.reverseOrder());

		for (int y : years) {
			List<Song> songsInYear = songs.stream().filter(s -> s.getYear() == y).collect(Collectors.toList());
			songsInYear.sort(Comparator.comparingDouble(Song::getSongHotttnesssDouble));
			Collections.reverse(songsInYear);
			//Collections.sort(songsInYear, Collections.reverseOrder());
			
			List<Song> first10Songs = songsInYear.stream().filter(s -> !s.getCityDetails().equals("HW3NA")).limit(SONGS_PER_YEAR).collect(Collectors.toList());
			/**
			List<Song> first10Songs = new ArrayList<Song>();
			
			for (Song s : songsInYear) {
				first10Songs.add(s);
			}
			**/
			for (Song s : first10Songs) {
				hottestSongsPerYear.add(s);
			}
		}

		if (!hottestSongsPerYear.isEmpty()) {
			for (Song s : hottestSongsPerYear) {
				context.write(new Text("Year: " + s.getYear()), new Text("Location: " + s.getCityDetails() + "\t\tSong Title: " + s.getSongTitle() + " by " + s.getArtistName() + "\t Song Hotttnesss: " + s.getSongHotttnesssDouble() + ", Artist Hotttnesss: " + s.getArtistHotttness()));
			}
		} else {
			context.write(new Text("No hottest songs per year were found"), new Text("Try again Daniel."));
		}
	}
}
