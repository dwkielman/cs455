package cs455.hadoop.hw3;

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

public class HW3Reducer extends Reducer<Text, Text, Text, Text> {
	
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
        			String songTermsArray[] = parts[1].split(" ");
        			for (int j = 0; j < songTermsArray.length; j++) {
        				terms.add(songTermsArray[j]);
        			}
        			//terms = DataUtilities.dataReader(parts[1]);
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
		
		/**
		 * Q7: Create segment data for the average song. Include start time, pitch, timbre, max loudness, max loudness time, and start loudness.
		 * 
		 */
		context.write(new Text("QUESTION 7: "), new Text("Create segment data for the average song. Include start time, pitch, timbre, max loudness, max loudness time, and start loudness."));
		
		if (startAverage.size() > 0) {
			double startAverageNumber = DataUtilities.getAverageValue(startAverage);
			context.write(new Text("Start Average"), new Text("Value: " + startAverageNumber));
		}
		
		if (pitchAverage.size() > 0) {
			double pitchAverageNumber = DataUtilities.getAverageValue(pitchAverage);
			context.write(new Text("Pitch Average"), new Text("Value: " + pitchAverageNumber));
		}
		
		if (timbreAverage.size() > 0) {
			double timbreAverageNumber = DataUtilities.getAverageValue(timbreAverage);
			context.write(new Text("Timbre Average"), new Text("Value: " + timbreAverageNumber));
		}
		
		if (loudnessMaxAverage.size() > 0) {
			double loudnessMaxAverageNumber = DataUtilities.getAverageValue(loudnessMaxAverage);
			context.write(new Text("Loudness Max Average"), new Text("Value: " + loudnessMaxAverageNumber));
		}
		
		if (loudnessStartAverage.size() > 0) {
			double loudnessStartAverageNumber = DataUtilities.getAverageValue(loudnessStartAverage);
			context.write(new Text("Loudness Start Average"), new Text("Value: " + loudnessStartAverageNumber));
		}
		
		/**
		 * Q8: Which artist is the most generic? Which artist is the most unique?
		 * 
		 */
		double songHotttnessAverage = 0.0;
		double artistFamiliarityAverage = 0.0;
		double artistHotttnesssAverage = 0.0;
		
		double lowestSongHottnesss = songHotttnessList.stream().min(Comparator.naturalOrder()).get();
		double highestArtistHottnesss = artistHotttnesssList.stream().max(Comparator.naturalOrder()).get();
		double lowestArtistHottnesss = artistHotttnesssList.stream().min(Comparator.naturalOrder()).get();
		double highestArtistFamiliarity = artistFamiliarityList.stream().max(Comparator.naturalOrder()).get();
		double lowestArtistFamiliarity = artistFamiliarityList.stream().min(Comparator.naturalOrder()).get();
		
		if (songHotttnessList.size() > 0) {
			songHotttnessAverage = DataUtilities.getAverageValue(songHotttnessList);
		}
		if (artistFamiliarityList.size() > 0) {
			artistFamiliarityAverage = DataUtilities.getAverageValue(artistFamiliarityList);
		}
		if (artistHotttnesssList.size() > 0) {
			artistHotttnesssAverage = DataUtilities.getAverageValue(artistHotttnesssList);
		}
		
		double averageArtistHottnesssMin = (artistHotttnesssAverage - 0.1);
		double averageArtistHottnesssMax = (artistHotttnesssAverage + 0.1);
		double averageSongHottnesssMin = (songHotttnessAverage - 0.1);
		double averageSongHottnesssMax = (songHotttnessAverage + 0.1);
		double averageArtistFamiliarityMin = (artistFamiliarityAverage - 0.1);
		double averageArtistFamiliarityMax = (artistFamiliarityAverage + 0.1);
		
		ArrayList<Song> genericSongs = new ArrayList<Song>();
		ArrayList<Song> uniqueSongs = new ArrayList<Song>();
		
		for (Song s : songs) {
			if (((s.getArtistHotttness() >= averageArtistHottnesssMin && s.getArtistHotttness() <= averageArtistHottnesssMax) ||
			(s.getSongHotttnesssDouble() >=  averageSongHottnesssMin && s.getSongHotttnesssDouble() <= averageSongHottnesssMax) ||
			((s.getArtistFamiliarity() >= averageArtistFamiliarityMin && s.getArtistFamiliarity() <= averageArtistFamiliarityMax) || s.getArtistFamiliarity() > (highestArtistFamiliarity - 0.1))) && 
			(s.getTimeSignatureDouble() % 2 == 0)) {
				genericSongs.add(s);
			} else if (((s.getArtistHotttness() > (highestArtistHottnesss - 0.1) || s.getArtistHotttness() < (lowestArtistHottnesss + 0.1)) || 
					(s.getSongHotttnesssDouble() > (maximumSongHotttnesss - 0.1) || s.getSongHotttnesssDouble() < (lowestSongHottnesss - 0.1)) ||
					(s.getArtistFamiliarity() < (lowestArtistFamiliarity + 0.1)))) {
						uniqueSongs.add(s);
			}
		}
		
		Map<String, List<Song>> genericSongsGrouped = genericSongs.stream().collect(Collectors.groupingBy(s -> s.getArtistID()));
		Map<String, List<Song>> uniqueSongsGrouped = uniqueSongs.stream().collect(Collectors.groupingBy(s -> s.getArtistID()));

		context.write(new Text("QUESTION 8: "), new Text("Which artist is the most generic? Which artist is the most unique?"));
		
		counter = 3;
		
		if (genericSongsGrouped.size() > 0) {
			if (genericSongsGrouped.size() < 3) {
				counter = genericSongsGrouped.size();
			}
			context.write(new Text("GENERIC ARTISTS:"), new Text("Determind by having songs that have average artist hotttness, average song hottness, high artist familiarity and a generic time signature of 2"));
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
			context.write(new Text("UNIQUE ARTISTS:"), new Text("Determind by having songs that have extreme high or low artist hottness, extreme high or low song hottness and low artist familiarity"));
			for (int i = 0; i < counter; i++) {
				String artistID = uniqueSongsGrouped.entrySet().stream().max((a1, a2) -> a1.getValue().size() > a2.getValue().size() ? 1 : -1).get().getKey();
				List<Song> songs = uniqueSongsGrouped.get(artistID);
				int numOfSongs = songs.size();
				String artistName = songs.get(0).getArtistName();
				context.write(new Text("Most Unique  Artist:"), new Text("Artst: " + artistName + " with " + numOfSongs + " unique Songs."));
				uniqueSongsGrouped.remove(artistID, songs);
			}
		}
		
		/**
		 * Q9: Imagine a song with a higher hotttnesss score than the song in your answer to Q3. List this songs tempo, 
		 * time signature, danceability, duration, mode, energy, key, loudness, when it stops fading in, when it starts 
		 * fading out, and which terms describe the artist who made it. Give both the song and the artist who made it unique names.
		 */
		ArrayList<Song> hottestSongs = new ArrayList<Song>();
		
		songs.sort(Comparator.comparingDouble(Song::getSongHotttnesssDouble));
		Collections.reverse(songs);

		for (Song s : songs) {
			if ((s.getSongHotttnesssDouble() <= maximumSongHotttnesss) && (s.getSongHotttnesssDouble() >= (maximumSongHotttnesss - 0.01))) {
				hottestSongs.add(s);
			}
		}
		
		if (!hottestSongs.isEmpty()) {
			double highestDanceability = hottestSongs.stream().max(Comparator.comparing(s -> ((Song) s).getDanceabilityDouble())).get().getDanceabilityDouble();
			double longestDuration = hottestSongs.stream().max(Comparator.comparing(s -> ((Song) s).getDurationDouble())).get().getDurationDouble();
			double longestFadeInTime = hottestSongs.stream().max(Comparator.comparing(s -> ((Song) s).getFadeInEndTime())).get().getFadeInEndTime();
			double highestEnergy = hottestSongs.stream().max(Comparator.comparing(s -> ((Song) s).getEnergyDouble())).get().getEnergyDouble();
			double highestSongKey = hottestSongs.stream().max(Comparator.comparing(s -> ((Song) s).getSongKey())).get().getSongKey();
			double highestMode = hottestSongs.stream().max(Comparator.comparing(s -> ((Song) s).getSongMode())).get().getSongMode();
			double highestTempo = hottestSongs.stream().max(Comparator.comparing(s -> ((Song) s).getSongTempo())).get().getSongTempo();
			double highestTimeSignature = hottestSongs.stream().max(Comparator.comparing(s -> ((Song) s).getTimeSignatureDouble())).get().getTimeSignatureDouble();
			double highestLoudness = hottestSongs.stream().max(Comparator.comparing(s -> ((Song) s).getLoudnessDouble())).get().getLoudnessDouble();
			double highestFadeOutDuration = hottestSongs.stream().max(Comparator.comparing(s -> ((Song) s).getFadeOutTime())).get().getFadeOutTime();
			double longestFadeOutStart = longestDuration - highestFadeOutDuration;
			
			ArrayList<String> hottestSongTerms = new ArrayList<String>();
			
			for (Song s : hottestSongs) {
				ArrayList<String> sTerms = s.getTerms();
				for (String t : sTerms) {
					if (!hottestSongTerms.contains(t)) {
						hottestSongTerms.add(t);
					}
				}
			}

			String termsString = "";
			
			if (!hottestSongTerms.isEmpty()) {
				termsString = String.join(", ", hottestSongTerms);
			}
			
			context.write(new Text("QUESTION 9: "), new Text("Imagine a song with a higher hotttnesss score than the song in your answer to Q3. List this songs tempo, "
					+ "time signature, danceability, duration, mode, energy, key, loudness, when it stops fading in, when it starts fading out, and which terms describe "
					+ "the artist who made it. Give both the song and the artist who made it unique names."));
			context.write(new Text("The hottest song was created via analysing " + hottestSongs.size() + " number of hottest songs."), new Text(""));
			context.write(new Text("Song Name: The Hottest Song Ever, Artist: The Hotties"), new Text("Song Metrics: Danceability: " + highestDanceability + "\tDuration: " + longestDuration + "\tFade In Ends: " + longestFadeInTime + "\tEnergy: " + highestEnergy
					 + "\tKey: " + highestSongKey + "\tMode: " + highestMode + "\tTempo: " + highestTempo + "\tTime Signature: " + highestTimeSignature + "\tLoudness: " + highestLoudness + "\tFade Out Starts: " + longestFadeOutStart
					 + "\tTerms: " + termsString));
		}
		
		/**
		 * Q10: Come up with an interesting question of your own to answer. This question should be more complex than Q7, Q8 or Q9. Answer it.
		 * For this component, think of yourself as the lead data scientist at a start-up firm. What would do with this dataset that is cool?
		 * You are allowed to: (1) combine your analysis with other datasets, (2) use other frameworks
		 */
		ArrayList<Song> hottestSongsPerYear = new ArrayList<Song>();
		
		songs.sort(Comparator.comparingInt(Song::getYear));	
		Collections.reverse(songs);
		
		years.sort(Comparator.naturalOrder());
		Collections.reverse(years);

		for (int y : years) {
			List<Song> songsInYear = songs.stream().filter(s -> s.getYear() == y).collect(Collectors.toList());
			songsInYear.sort(Comparator.comparingDouble(Song::getSongHotttnesssDouble));
			Collections.reverse(songsInYear);
			
			List<Song> first10Songs = songsInYear.stream().filter(s -> !s.getCityDetails().equals("HW3NA")).limit(SONGS_PER_YEAR).collect(Collectors.toList());

			for (Song s : first10Songs) {
				hottestSongsPerYear.add(s);
			}
		}

		context.write(new Text("QUESTION 10: "), new Text("Come up with an interesting question of your own to answer. This question should be more complex than Q7, Q8 or Q9. Answer it."
				+ "Do the hottest songs in a given year originate from a singular location? Essentially, is there a correlation between the top 10 hottest songs in a given year and their location?"
				+ "\nI have below the list of the 10 hottest songs in a given year, their locations and metrics on said songs."));
		if (!hottestSongsPerYear.isEmpty()) {
			for (Song s : hottestSongsPerYear) {
				context.write(new Text("Year: " + s.getYear()), new Text("Location: " + s.getCityDetails() + "\t\tSong Title: " + s.getSongTitle() + " by " + s.getArtistName() + "\t Song Hotttnesss: " + s.getSongHotttnesssDouble() + ", Artist Hotttnesss: " + s.getArtistHotttness()));
			}
		} else {
			context.write(new Text("No hottest songs per year were found"), new Text("Try again Daniel."));
		}
	}

}
