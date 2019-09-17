package cs455.hadoop.q08;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.hadoop.Util.Artist;
import cs455.hadoop.Util.DataUtilities;
import cs455.hadoop.Util.Song;

public class Q8Reducer extends Reducer<Text, Text, Text, Text>  {

	private ArrayList<Song> songs = new ArrayList<Song>();
	private HashMap<Artist, ArrayList<Song>> artists = new HashMap<Artist, ArrayList<Song>>();
	private float maxFamiliarity = -1;
	private float minFamiliarity = -1;
	private long familiarityTotal = 0;
	private long familiarityCount = 0;
	private long artistHottnesssCount = 0;
	private long songHottnesssCount = 0;
	private long songHottnesssTotal = 0;
	private int maxNumberOfSimilarArtists = 0;
	private long numberOfSimilarArtistsCount = 0;
	private float maxArtistHottnesss = -1;
	private float minArtistHottnesss = -1;
	private long artistHottnesssTotal = 0;
	private float maxSongHottnesss = -1;
	private float minSongHottnesss = -1;
	private final int SONGS_TO_WRITE = 3;
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String songTitle = "";
        String artistID = "";
        String artistName = "";
        double songHotttnesss = 0.0;
        double timeSignature = 0.0;
        double artistFamiliarity = 0.0;
        double artistHotttnesss = 0.0;
        int numberOfSimilarArtists = 0;
        
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
        		} else if (parts[0].equals("SONGHOTTTNESSS")) {
        			songHotttnesss = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("TIMESIGNATURE")) {
        			timeSignature = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("ARTISTFAMILIARITY")) {
        			artistFamiliarity = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("ARTISTHOTTTNESSS")) {
        			artistHotttnesss = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("SIMILARARTISTS")) {
        			numberOfSimilarArtists = (int) DataUtilities.doubleReader(parts[1]);
        		} 
        	}
        }
        
		Song song = new Song(key.toString(), songTitle);
		Artist artist = new Artist(artistID, artistName);
		song.setSongHotttnesssDouble(songHotttnesss);
		song.setTimeSignatureDouble(timeSignature);
		song.setArtistFamiliarity(artistFamiliarity);
		song.setArtistHotttness(artistHotttnesss);
		song.setNumberOfSimilarArtists(numberOfSimilarArtists);
		artist.setArtistFamiliarity(artistFamiliarity);
		artist.setArtistHotttnesss(artistHotttnesss);
		artist.setNumberOfSimilarArtists(numberOfSimilarArtists);
		song.setArtistID(artistID);
		song.setArtistName(artistName);

		songs.add(song);
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
        float songHotttnesss = 0;
		float timeSignature = 0;
		String artistID = "";
		String artistName = "";
        String songTitle = "";
		float artistFamiliarity = 0;
		float artistHottnesss = 0;
		int numberOfSimilarArtists = 0;
        
        for (Text val : values) {
        	String parts[] = val.toString().split(",");
        	songHotttnesss = Float.parseFloat(parts[0]);
        	timeSignature = Float.parseFloat(parts[1]);
        	artistID = parts[2];
        	artistName = parts[3];
        	songTitle = parts[4];
        	artistFamiliarity = Float.parseFloat(parts[5]);
        	artistHottnesss = Float.parseFloat(parts[6]);
        	numberOfSimilarArtists = parts[7].toString().split(" ").length;
        	
        	// song hottnesss
        	if (songHotttnesss != 0) {
        		songHottnesssCount++;
        		songHottnesssTotal += songHotttnesss;
        		// max song hottnesss
        		if (songHotttnesss > maxSongHottnesss) {
            		maxSongHottnesss = songHotttnesss;
            	}
        		// min song hottnesss
            	if (minSongHottnesss == -1) {
            		minSongHottnesss = songHotttnesss;
            	}else if (songHotttnesss < minSongHottnesss && songHotttnesss != 0) {
            		minSongHottnesss = songHotttnesss;
            	}
        	}

        	// artist familiarity
        	if (artistFamiliarity != 0) {
        		familiarityCount++;
        		familiarityTotal += artistFamiliarity;
        		// max artist familiarity
        		if (artistFamiliarity > maxFamiliarity) {
            		maxFamiliarity = artistFamiliarity;
            	}
        		// min artist familiarity
            	// default first familiarity to first familiarity value > 0
            	if (minFamiliarity == -1) {
            		minFamiliarity = artistFamiliarity;
            	}else if (artistFamiliarity < minFamiliarity) {
            		minFamiliarity = artistFamiliarity;
            	}
        	}
        	
        	// artist hottnesss
        	if (artistHottnesss != 0) {
        		artistHottnesssCount++;
        		artistHottnesssTotal += artistHottnesss;
        		// max artist hottnesss
        		if (artistHottnesss > maxArtistHottnesss) {
        			maxArtistHottnesss = artistHottnesss;
            	}
        		// min artist hottnesss
            	// default first artist hottnesss to first artist hottnesss value > 0
            	if (minArtistHottnesss == -1) {
            		minArtistHottnesss = artistHottnesss;
            	}else if (artistHottnesss < minArtistHottnesss) {
            		minArtistHottnesss = artistHottnesss;
            	}
        	}
        	
        	if (numberOfSimilarArtists > 0) {
        		numberOfSimilarArtistsCount++;
        		if (numberOfSimilarArtists > maxNumberOfSimilarArtists) {
        			maxNumberOfSimilarArtists = numberOfSimilarArtists;
        		}
        	}
        }

		// only want songs that have all of the values we compare against so will only add to our database of Songs if all values are present
		if (songHotttnesss > 0 && timeSignature > 0 && artistHottnesss > 0 && numberOfSimilarArtists > 0) {
			Song song = new Song(key.toString(), songTitle);
			song.setSongHotttnesss(songHotttnesss);
			song.setTimeSignature(timeSignature);
			song.setArtistID(artistID);
			song.setArtistName(artistName);
			song.setArtistFamiliarity(artistFamiliarity);
			song.setSongTitle(songTitle);
			song.setArtistHotttness(artistHottnesss);
			song.setNumberOfSimilarArtists(numberOfSimilarArtists);

			songs.add(song);
		}
		**/
    }
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		ArrayList<Double> songHotttnessList = new ArrayList<Double>();
		ArrayList<Double> timeSignatureList = new ArrayList<Double>();
		ArrayList<Double> artistFamiliarityList = new ArrayList<Double>();
		ArrayList<Double> artistHotttnesssList = new ArrayList<Double>();
		ArrayList<Double> numberOfSimilarArtistsList = new ArrayList<Double>();
		
		double songHotttnessAverage = 0.0;
		double timeSignatureAverage = 0.0;
		double artistFamiliarityAverage = 0.0;
		double artistHotttnesssAverage = 0.0;
		double similarArtistsAverage = 0.0;
		
		double highestSongHottnesss = 0.0;
		double lowestSongHottnesss = Double.MAX_VALUE;
		double highestArtistHottnesss = 0.0;
		double lowestArtistHottnesss = Double.MAX_VALUE;
		double highestArtistFamiliarity = 0.0;
		double lowestArtistFamiliarity = Double.MAX_VALUE;
		
		for (Song s : songs) {
			if (s.getSongHotttnesssDouble() > 0.0) {
				songHotttnessList.add(s.getSongHotttnesssDouble());
				if (s.getSongHotttnesssDouble() > highestSongHottnesss) {
					highestSongHottnesss = s.getSongHotttnesssDouble();
				} else if (s.getSongHotttnesssDouble() < lowestSongHottnesss) {
					highestSongHottnesss = s.getSongHotttnesssDouble();
				}
			}
			if (s.getTimeSignatureDouble() > 0.0) {
				timeSignatureList.add(s.getTimeSignatureDouble());
			}
		}
		
		if (songHotttnessList.size() > 0) {
			songHotttnessAverage = DataUtilities.getAverageValue(songHotttnessList);
		}
		if (timeSignatureList.size() > 0) {
			timeSignatureAverage = DataUtilities.getAverageValue(timeSignatureList);
		}
		
		for (Map.Entry<Artist, ArrayList<Song>> entry : artists.entrySet()) {
        	Artist a = entry.getKey();
        	if (a.getArtistFamiliarity() > 0.0) {
        		artistFamiliarityList.add(a.getArtistFamiliarity());
        		if (a.getArtistFamiliarity() > highestArtistFamiliarity) {
        			highestArtistFamiliarity = a.getArtistFamiliarity();
				} else if (a.getArtistFamiliarity() < lowestArtistFamiliarity) {
					lowestArtistFamiliarity = a.getArtistFamiliarity();
				}
        	}
        	if (a.getArtistHotttnesss() > 0.0) {
        		artistHotttnesssList.add(a.getArtistHotttnesss());
        		if (a.getArtistHotttnesss() > highestArtistHottnesss) {
        			highestArtistHottnesss = a.getArtistHotttnesss();
				} else if (a.getArtistHotttnesss() < lowestArtistHottnesss) {
					lowestArtistHottnesss = a.getArtistHotttnesss();
				}
        	}
        	if (a.getNumberOfSimilarArtists() > 0) {
        		numberOfSimilarArtistsList.add((double) a.getNumberOfSimilarArtists());
        	}
        }
		
		if (artistFamiliarityList.size() > 0) {
			artistFamiliarityAverage = DataUtilities.getAverageValue(artistFamiliarityList);
		}
		if (artistHotttnesssList.size() > 0) {
			artistHotttnesssAverage = DataUtilities.getAverageValue(artistHotttnesssList);
		}
		if (numberOfSimilarArtistsList.size() > 0) {
			similarArtistsAverage = DataUtilities.getAverageValue(numberOfSimilarArtistsList);
		}
		
		double averageArtistHottnesssMin = (artistHotttnesssAverage - 0.1);
		double averageArtistHottnesssMax = (artistHotttnesssAverage + 0.1);
		double averageSongHottnesssMin = (songHotttnessAverage - 0.1);
		double averageSongHottnesssMax = (songHotttnessAverage + 0.1);
		double averageArtistFamiliarityMin = (artistFamiliarityAverage - 0.1);
		double averageArtistFamiliarityMax = (artistFamiliarityAverage + 0.1);

		/**
		double averageArtistHottnesssMin = ((artistHottnesssTotal / artistHottnesssCount) - 0.1);
		double averageArtistHottnesssMax = ((artistHottnesssTotal / artistHottnesssCount) + 0.1);
		double averageSongHottnesssMin = (songHottnesssTotal / songHottnesssCount) - 0.1;
		double averageSongHottnesssMax = (songHottnesssTotal / songHottnesssCount) + 0.1;
		double averageArtistFamiliarityMin = (familiarityTotal / familiarityCount) - 0.1;
		double averageArtistFamiliarityMax = (familiarityTotal / familiarityCount) + 0.1;
		
		float highestSongHottnesss = songs.stream().max(Comparator.comparing(s -> ((Song) s).getSongHotttnesss())).get().getSongHotttnesss();
		float lowestSongHottnesss = songs.stream().min(Comparator.comparing(s -> ((Song) s).getSongHotttnesss())).get().getSongHotttnesss();
		
		float highestArtistHottnesss = songs.stream().max(Comparator.comparing(s -> ((Song) s).getArtistHotttness())).get().getArtistHotttness();
		float lowestArtistHottnesss = songs.stream().min(Comparator.comparing(s -> ((Song) s).getArtistHotttness())).get().getArtistHotttness();
		
		float highestArtistFamiliarity = songs.stream().max(Comparator.comparing(s -> ((Song) s).getArtistFamiliarity())).get().getArtistFamiliarity();
		float lowestArtistFamiliarity = songs.stream().min(Comparator.comparing(s -> ((Song) s).getArtistFamiliarity())).get().getArtistFamiliarity();
		 **/
		
		
		ArrayList<Song> genericSongs = new ArrayList<Song>();
		ArrayList<Song> uniqueSongs = new ArrayList<Song>();
		//ArrayList<Artist> genericArtists = new ArrayList<Artist>();
		//ArrayList<Artist> uniqueArtists = new ArrayList<Artist>();

		for (Song s : songs) {
			if (((s.getArtistHotttness() >= averageArtistHottnesssMin && s.getArtistHotttness() <= averageArtistHottnesssMax) ||
			(s.getSongHotttnesssDouble() >=  averageSongHottnesssMin && s.getSongHotttnesssDouble() <= averageSongHottnesssMax) ||
			((s.getArtistFamiliarity() >= averageArtistFamiliarityMin && s.getArtistFamiliarity() <= averageArtistFamiliarityMax) || s.getArtistFamiliarity() > (highestArtistFamiliarity - 0.1))) && 
			(s.getTimeSignatureDouble() % 2 == 0)) {
				genericSongs.add(s);
			} else if (((s.getArtistHotttness() > (highestArtistHottnesss - 0.1) || s.getArtistHotttness() < (lowestArtistHottnesss + 0.1)) || 
					(s.getSongHotttnesssDouble() > (highestSongHottnesss - 0.1) || s.getSongHotttnesssDouble() < (lowestSongHottnesss - 0.1)) ||
					(s.getArtistFamiliarity() < (lowestArtistFamiliarity + 0.1)))) { //&& (s.getTimeSignatureDouble() % 2 > 0)) {
						uniqueSongs.add(s);
			}
		}
		
		genericSongs.sort(Comparator.comparingInt(Song::getNumberOfSimilarArtists));
		Collections.reverse(genericSongs);
		
		uniqueSongs.sort(Comparator.comparingInt(Song::getNumberOfSimilarArtists));
		
		// Map<String, List<Student>> studlistGrouped = studlist.stream().collect(Collectors.groupingBy(w -> w.stud_location));
		Map<String, List<Song>> genericSongsGrouped = genericSongs.stream().collect(Collectors.groupingBy(s -> s.getArtistID()));
		Map<String, List<Song>> uniqueSongsGrouped = uniqueSongs.stream().collect(Collectors.groupingBy(s -> s.getArtistID()));

		int counter = 3;
		
		if (genericSongsGrouped.size() > 0) {
			if (genericSongsGrouped.size() < 3) {
				counter = genericSongsGrouped.size();
			}
			for (int i = 0; i < counter; i++) {
				String artistID = genericSongsGrouped.entrySet().stream().max((a1, a2) -> a1.getValue().size() > a2.getValue().size() ? 1 : -1).get().getKey();
				List<Song> songs = genericSongsGrouped.get(artistID);
				int numOfSongs = songs.size();
				String artistName = songs.get(0).getArtistName();
				context.write(new Text("Most Generic Artist:"), new Text("Artst: " + artistName + " with " + numOfSongs + " generic Songs."));
				genericSongsGrouped.remove(artistID, songs);
			}
		}
		
		counter = 3;
		
		if (uniqueSongsGrouped.size() > 0) {
			if (uniqueSongsGrouped.size() < 3) {
				counter = uniqueSongsGrouped.size();
			}
			for (int i = 0; i < counter; i++) {
				String artistID = uniqueSongsGrouped.entrySet().stream().max((a1, a2) -> a1.getValue().size() > a2.getValue().size() ? 1 : -1).get().getKey();
				List<Song> songs = uniqueSongsGrouped.get(artistID);
				int numOfSongs = songs.size();
				String artistName = songs.get(0).getArtistName();
				context.write(new Text("Most Unique  Artist:"), new Text("Artst: " + artistName + " with " + numOfSongs + " unique Songs."));
				uniqueSongsGrouped.remove(artistID, songs);
			}
		}
		
		
		
	}

}
