package cs455.hadoop.hw3a;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.hadoop.Util.Artist;
import cs455.hadoop.Util.DataUtilities;
import cs455.hadoop.Util.Song;

public class HW3aReducer extends Reducer<Text, Text, Text, Text> {
	
	private ArrayList<Song> songs;
	private ArrayList<Double> startAverage;
	private ArrayList<Double> pitchAverage;
	private ArrayList<Double> timbreAverage;
	private ArrayList<Double> loudnessMaxAverage;
	private ArrayList<Double> loudnessStartAverage;
	private ArrayList<Double> songHotttnessList;
	private ArrayList<Double> artistFamiliarityList;
	private ArrayList<Double> artistHotttnesssList;
	private List<Integer> years;
	private final static int SONGS_PER_YEAR = 10;
	
	@Override
    public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		songs = new ArrayList<Song>();
		startAverage = new ArrayList<Double>();
		pitchAverage = new ArrayList<Double>();
		timbreAverage = new ArrayList<Double>();
		loudnessMaxAverage = new ArrayList<Double>();
		loudnessStartAverage = new ArrayList<Double>();
		songHotttnessList = new ArrayList<Double>();
		artistFamiliarityList = new ArrayList<Double>();
		artistHotttnesssList = new ArrayList<Double>();
		years = new ArrayList<Integer>();
	}
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String songTitle = "";
        String artistID = "";
        String artistName = "";
        double loudness = 0.0;
        double songHotttnesss = 0.0;
        double totalFadeTime = 0;
        double duration = 0;
        double fadeInDuration = 0;
        double startOfFadeOut = 0;
        double danceability = 0;
        double energy = 0;
        double segmentsStartAverage = 0.0;
        double segmentsPitchesAverage = 0.0;
        double segmentsTimbreAverage = 0.0;
        double segmentsLoudnessMaxTimeAverage = 0.0;
        double segmentsLoudnessStartAverage = 0.0;
        double timeSignature = 0.0;
        double artistFamiliarity = 0.0;
        double artistHotttnesss = 0.0;
        int numberOfSimilarArtists = 0;
        double songKey = 0.0;
        double songMode = 0.0;
        double songTempo = 0.0;
        String location = "";
        int year = 0;
        ArrayList<String> terms = new ArrayList<String>();
        
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
        		} else if (parts[0].equals("LOUDNESS")) {
        			loudness = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("SONGHOTTTNESSS")) {
        			songHotttnesss = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("DURATION")) {
        			duration = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("ENDOFFADEIN")) {
        			fadeInDuration = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("STARTOFFADEOUT")) {
        			startOfFadeOut = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("DANCEABILITY")) {
        			danceability = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("ENERGY")) {
        			energy = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("SEGMENTSSTARTAVERAGE")) {
        			segmentsStartAverage = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("SEGMENTSPITCHESAVERAGE")) {
        			segmentsPitchesAverage = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("SEGMENTSTIMBREAVERAGE")) {
        			segmentsTimbreAverage = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("SEGMENTSLOUDNESSMAXTIMEAVERAGE")) {
        			segmentsLoudnessMaxTimeAverage = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("SEGMENTSLOUDNESSSTARTAVERAGE")) {
        			segmentsLoudnessStartAverage = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("TIMESIGNATURE")) {
        			timeSignature = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("ARTISTFAMILIARITY")) {
        			artistFamiliarity = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("ARTISTHOTTTNESSS")) {
        			artistHotttnesss = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("SIMILARARTISTS")) {
        			numberOfSimilarArtists = (int) DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("SONGKEY")) {
        			songKey = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("SONGMODE")) {
        			songMode = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("TEMPO")) {
        			songTempo = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("SONGTERMS")) {
        			terms = DataUtilities.dataReader(parts[1]);
        		} else if (parts[0].equals("LOCATION")) {
        			if (parts[1].equals("b''")) {
        				location = "HW3NA";
        			} else {
        				location = parts[1];
        			}
        		} else if (parts[0].equals("ARTISTYEAR")) {
        			year = (int) DataUtilities.doubleReader(parts[1]);
        			if (!years.contains(year) && year != 0) {
                		years.add(year);
                	}
        		} 
        	}
        }
        
		Song song = new Song(key.toString(), songTitle);
		song.setArtistID(artistID);
		song.setArtistName(artistName);
		song.setLoudnessDouble(loudness);
		song.setSongHotttnesssDouble(songHotttnesss);
		double fadeOutDuration = duration - startOfFadeOut;
		totalFadeTime = fadeInDuration + fadeOutDuration;
		song.setFadeInEndTime(fadeInDuration);
		song.setFadeOutTime(fadeOutDuration);
		song.incrementTotalFadeTime(totalFadeTime);
		song.setDurationDouble(duration);
		song.setEnergyDouble(energy);
		song.setDanceabilityDouble(danceability);
		song.setTimeSignatureDouble(timeSignature);
		song.setArtistFamiliarity(artistFamiliarity);
		song.setArtistHotttness(artistHotttnesss);
		song.setNumberOfSimilarArtists(numberOfSimilarArtists);
		song.setSongKey(songKey);
		song.setSongMode(songMode);
		song.setSongTempo(songTempo);
		song.setCityDetails(location);
		song.setYear(year);
		
		// only include unique terms for a Song and exclude null entries
		for (int i=0; i < terms.size(); i++) {
			if (!song.getTerms().contains(terms.get(i)) && !terms.get(i).equals("HW3NA")) {
				song.addTerm(terms.get(i));
			}
		}
		
		songs.add(song);
		
		// cache certain metrics for solving in cleanup
		if (segmentsStartAverage > 0.0) {
        	startAverage.add(segmentsStartAverage);
        }
        
        if (segmentsPitchesAverage > 0.0) {
        	pitchAverage.add(segmentsPitchesAverage);
        }
        
        if (segmentsTimbreAverage > 0.0) {
        	timbreAverage.add(segmentsTimbreAverage);
        }
        
        if (segmentsLoudnessMaxTimeAverage > 0.0) {
        	loudnessMaxAverage.add(segmentsLoudnessMaxTimeAverage);
        }
        
        if (segmentsLoudnessStartAverage > 0.0) {
        	loudnessStartAverage.add(segmentsLoudnessStartAverage);
        }
        
        if (songHotttnesss > 0.0) {
        	songHotttnessList.add(songHotttnesss);
        }
	}
	
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Map<String, List<Song>> artistsGrouped = songs.stream().collect(Collectors.groupingBy(s -> s.getArtistID()));
		
		// builds a map of artists and their songs used throughout finding solutions
		HashMap<Artist, List<Song>> artists = new HashMap<Artist, List<Song>>();
		for (Map.Entry<String, List<Song>> entry : artistsGrouped.entrySet()) {
			String artistName = entry.getValue().get(0).getArtistName();
			Artist a = new Artist(entry.getKey(), artistName);
			a.setArtistFamiliarity(entry.getValue().get(0).getArtistFamiliarity());
			a.setArtistHotttnesss(entry.getValue().get(0).getArtistHotttness());
			a.setNumberOfSimilarArtists(entry.getValue().get(0).getNumberOfSimilarArtists());

	        if (entry.getValue().get(0).getArtistFamiliarity() > 0.0) {
	        	artistFamiliarityList.add(entry.getValue().get(0).getArtistFamiliarity());
	        }
	        
	        if (entry.getValue().get(0).getArtistHotttness() > 0.0) {
	        	artistHotttnesssList.add(entry.getValue().get(0).getArtistHotttness());
	        }
	        
			artists.put(a, entry.getValue());
		}
		
		/**
		 * Q1: Which artist has the most songs in the data set?
		 */
		Artist artistWithMostSongs = artists.entrySet().stream().max(Comparator.comparingInt(entry -> entry.getValue().size())).get().getKey();
		int mostNumberOfSongs = artists.get(artistWithMostSongs).size();
		
		context.write(new Text("QUESTION 1: "), new Text("Which artist has the most songs in the data set?"));
		context.write(new Text(artistWithMostSongs.getArtistName()), new Text("" + mostNumberOfSongs));
		
		/**
		 * Q2: Which artists songs are the loudest on average?
		 */
        ArrayList<Artist> loudnessArtists = new ArrayList<Artist>();
        // calcaulte the average loudness for a given artist
        for (Map.Entry<Artist, List<Song>> entry : artists.entrySet()) {
        	String id = entry.getKey().getArtistID();
        	String name = entry.getKey().getArtistName();
        	double totalLoudness = 0.0;
            int numberOfLouds = 0;
        	for (Song s : entry.getValue()) {
        		if (s.getLoudnessDouble() < 0.0) {
					totalLoudness += s.getLoudnessDouble();
					numberOfLouds++;
				}
        	}
        	double loudnessAverage = 0.0;
        	if (numberOfLouds > 0 && totalLoudness < 0.0) {
        		loudnessAverage = totalLoudness / numberOfLouds;
        		Artist a = new Artist(id, name);
        		a.setAverageLoudness(loudnessAverage);
        		loudnessArtists.add(a);
        	}
        }
        
        // get the artist with the max loudness average
        Artist loudestArtist = loudnessArtists.stream().max(Comparator.comparingDouble(Artist::getAverageLoudness)).get();
		context.write(new Text("QUESTION 2: "), new Text("Which artists songs are the loudest on average?"));
		context.write(new Text("Artist Name: " + loudestArtist.getArtistName()), new Text("Average Highest Loudness: " + loudestArtist.getAverageLoudness()));
		
		/**
		 * Q3: What is the song with the highest hotttnesss (popularity) score?
		 */
		double maximumSongHotttnesss = songs.stream().max(Comparator.comparingDouble(Song::getSongHotttnesssDouble)).get().getSongHotttnesssDouble();
		context.write(new Text("QUESTION 3: "), new Text("What is the song with the highest hotttnesss (popularity) score?"));
		context.write(new Text("The highest Song hotttnesss found was the following value: "), new Text(String.valueOf(maximumSongHotttnesss)));
		context.write(new Text("Here is the top song followed by a few others that have reached this hotttnesss level:"), new Text("\t"));
		songs.sort(Comparator.comparingDouble(Song::getSongHotttnesssDouble));
		Collections.reverse(songs);
		for (int i = 0; i < 3; i++) {
			context.write(new Text(songs.get(i).getSongTitle()), new Text("Song is at hotttnesss: " + songs.get(i).getSongHotttnesssDouble()));
		}
		
		/**
		 * Q4: Which artist has the highest total time spent fading in their songs?
		 * 
		 */
		ArrayList<Artist> artistsFadingList = new ArrayList<Artist>();
		for (Map.Entry<Artist, List<Song>> entry : artists.entrySet()) {
        	Artist a = entry.getKey();
        	//List<Song> ss = entry.getValue();
        	for (Song s : entry.getValue()) {
        		a.incrementTotalTimeFading(s.getTotalFadeTime());
        	}
        	artistsFadingList.add(a);
        }
        
		artistsFadingList.sort(Comparator.comparingDouble(Artist::getTotalTimeFading));
		Collections.reverse(artistsFadingList);
		
		context.write(new Text("QUESTION 4: "), new Text("Which artist has the highest total time spent fading in their songs?"));
		context.write(new Text("Here is the top artist who loves to fade followed by a few others that also enjoy fading:"), new Text("\t"));
		
		int counter = 3;
		if (!artistsFadingList.isEmpty()) {
			if (artistsFadingList.size() < 3) {
				counter = artistsFadingList.size();
			}
			for (int i=0; i < counter; i++) {
				context.write(new Text(artistsFadingList.get(i).getArtistName()), new Text("Really loves to fade this much: " + artistsFadingList.get(i).getTotalTimeFading()));
			}
		}
		
		// clear this for data
		artistsFadingList = new ArrayList<Artist>();
		
		/**
		 * Q5: What is the longest song(s)? The shortest song(s)? The song(s) of median length?
		 * 
		 */
		double maxSongDuration = 0;
		double minSongDuration = Double.MAX_VALUE;
		double averageSongDuration = 0;
		double totalSongDuration = 0;
		int totalNumberOfSongsDurations = 0;
		
		// find the max and min song duration
		for (Song s : songs) {
			double songDuration = s.getDurationDouble();
			if (songDuration > 0) {
				if (songDuration > maxSongDuration) {
					maxSongDuration = songDuration;
				}
				if (songDuration < minSongDuration) {
					minSongDuration = songDuration;
				}
				totalSongDuration += songDuration;
				totalNumberOfSongsDurations++;
			}
		}
		
		if (totalNumberOfSongsDurations > 0) {
			averageSongDuration = totalSongDuration / totalNumberOfSongsDurations;
		}
		
		ArrayList<Song> longestDurationSongsList = new ArrayList<Song>();
		ArrayList<Song> shortestDurationSongsList = new ArrayList<Song>();
		ArrayList<Song> averageDurationSongsList = new ArrayList<Song>();
		
		for (Song s : songs) {
			if (s.getDurationDouble() == maxSongDuration) {
				longestDurationSongsList.add(s);
			} else if (s.getDurationDouble() == minSongDuration) {
				shortestDurationSongsList.add(s);
			} else if (DataUtilities.roundDouble(s.getDurationDouble(), 1) == DataUtilities.roundDouble(averageSongDuration, 1)) {
				averageDurationSongsList.add(s);
			}
		}
		
		context.write(new Text("QUESTION 5: "), new Text("What is the longest song(s)? The shortest song(s)? The song(s) of median length?"));
		context.write(new Text("LONGEST SONGS: "), new Text("The Longest Song Duration is: " + maxSongDuration + "\tTotal number of songs that are at the longest Duration: " + longestDurationSongsList.size()));
		
		int songCounter = 3;
		
		if (longestDurationSongsList.size() > 0) {
			if (longestDurationSongsList.size() < 3) {
				songCounter = longestDurationSongsList.size();
			}
			for (int i=0; i < songCounter; i++) {
				context.write(new Text(longestDurationSongsList.get(i).getSongTitle()), new Text("Song Duration: " + longestDurationSongsList.get(i).getDurationDouble()));
			}
		}
		
		context.write(new Text("SHORTEST SONGS: "), new Text("The Shortest Song Duration is: " + minSongDuration + "\tTotal number of songs that are at the shortest Duration: " + shortestDurationSongsList.size()));
		
		songCounter = 3;
		
		if (shortestDurationSongsList.size() > 0) {
			if (shortestDurationSongsList.size() < 3) {
				songCounter = shortestDurationSongsList.size();
			}
			for (int i=0; i < songCounter; i++) {
				context.write(new Text(shortestDurationSongsList.get(i).getSongTitle()), new Text("Song Duration: " + shortestDurationSongsList.get(i).getDurationDouble()));
			}
		}
		
		context.write(new Text("AVERAGE SONGS: "), new Text("The Average Song Duration is: " + averageSongDuration + "\tTotal number of songs that are at the average Duration: " + averageDurationSongsList.size()));
		
		songCounter = 3;
		
		if (averageDurationSongsList.size() > 0) {
			if (averageDurationSongsList.size() < 3) {
				songCounter = averageDurationSongsList.size();
			}
			averageDurationSongsList.sort(Comparator.comparingDouble(Song::getDurationDouble));
			Collections.reverse(averageDurationSongsList);
			for (int i=0; i < songCounter; i++) {
				context.write(new Text(averageDurationSongsList.get(i).getSongTitle()), new Text("Is of duratrion: " + averageDurationSongsList.get(i).getDurationDouble()));
			}
		}
		
		/**
		 * Q6: What are the 10 most energetic and danceable songs? List them in descending order.
		 * 
		 */
		songs.stream().sorted(Comparator.comparing(Song::getDanceabilityDouble).thenComparing(Song::getEnergyDouble));
		Collections.reverse(songs);
		
		ArrayList<Song> danceableSongs = new ArrayList<Song>();
		
		for (Song s : songs) {
			if (s.getDanceabilityDouble() > 0 && s.getEnergyDouble() > 0) {
				danceableSongs.add(s);
			}
		}
		
		counter = 10;
		
		context.write(new Text("QUESTION 6: "), new Text("What are the 10 most energetic and danceable songs? List them in descending order."));
		context.write(new Text("Most Danceable and Energetic Songs: "), new Text("We have this many songs that are the highly danceable and energetic " + danceableSongs.size()));
		
		if (danceableSongs.size() > 0) {
			if (danceableSongs.size() < 10) {
				counter = danceableSongs.size();
			}
			for (int i=0; i < counter; i++) {
				context.write(new Text(danceableSongs.get(i).getSongTitle()), new Text("Danceability: " + danceableSongs.get(i).getDanceabilityDouble() + ", Energy: " + danceableSongs.get(i).getEnergyDouble()));
			}
		} else {
			context.write(new Text("No Danceable or Energetic Songs Found."), new Text("Due to there being " + danceableSongs.size() + " number of Danceable & Energetic Songs"));
		}
	}

}
