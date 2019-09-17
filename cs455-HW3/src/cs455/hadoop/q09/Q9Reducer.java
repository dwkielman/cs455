package cs455.hadoop.q09;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.hadoop.Util.DataUtilities;
import cs455.hadoop.Util.Song;

public class Q9Reducer extends Reducer<Text, Text, Text, Text>  {

	private double maxSongHotttnesss = -1;
	private ArrayList<Song> songs = new ArrayList<Song>();
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String songTitle = "";
        String artistID = "";
        String artistName = "";
        double duration = 0;
        double fadeInDuration = 0;
        double startOfFadeOut = 0;
        double songHotttnesss = 0.0;
        double danceability = 0.0;
        double energy = 0.0;
        double songKey = 0.0;
        double songMode = 0.0;
        double songTempo = 0.0;
        double timeSignature = 0.0;
        double loudness = 0.0;
        ArrayList<String> terms = new ArrayList<String>();
		
        for (Text val : values) {
        	ArrayList<String> record = DataUtilities.dataReader(val.toString());
        	//String[] record = val.toString().split(",");
        	for (int i = 0; i < record.size(); i++) {
        		//for (int i = 0; i < record.length; i++) {
        		//String parts[] = record[i].split("_#_");
        		String parts[] = record.get(i).split("_#_");
        		if (parts[0].equals("ARTISTID")) {
        			artistID = parts[1];
        		} else if (parts[0].equals("ARTISTNAME")) {
        			artistName = parts[1];
        		} else if (parts[0].equals("SONGTITLE")) {
        			songTitle = parts[1];
        		} else if (parts[0].equals("DURATION")) {
        			duration = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("ENDOFFADEIN")) {
        			fadeInDuration = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("STARTOFFADEOUT")) {
        			startOfFadeOut = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("SONGHOTTTNESSS")) {
        			songHotttnesss = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("DANCEABILITY")) {
        			danceability = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("ENERGY")) {
        			energy = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("SONGKEY")) {
        			songKey = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("SONGMODE")) {
        			songMode = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("TEMPO")) {
        			songTempo = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("TIMESIGNATURE")) {
        			timeSignature = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("LOUDNESS")) {
        			loudness = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("SONGTERMS")) {
        			terms = DataUtilities.dataReader(parts[1]);
        		}
        	}
        }
		/**
        float songHotttnesss = 0;
		float danceability = 0;
		float duration = 0;
		float fadeInEnd = 0;
		float energy = 0;
		float songKey = 0;
		float mode = 0;
		float tempo = 0;
		float timeSignature = 0;
		float loudness = 0;
		float fadeOutStart = 0;
        String songTitle = "";
		String artistTerms = "";
        
        for (Text val : values) {
        	String parts[] = val.toString().split(",");
        	songHotttnesss = Float.parseFloat(parts[0]);
        	danceability = Float.parseFloat(parts[1]);
        	duration = Float.parseFloat(parts[2]);
        	fadeInEnd = Float.parseFloat(parts[3]);
        	energy = Float.parseFloat(parts[4]);
        	songKey = Float.parseFloat(parts[5]);
        	mode = Float.parseFloat(parts[6]);
        	tempo = Float.parseFloat(parts[7]);
        	timeSignature = Float.parseFloat(parts[8]);
        	loudness = Float.parseFloat(parts[9]);
        	fadeOutStart = Float.parseFloat(parts[10]);
        	songTitle = parts[11];
        	artistTerms = parts[12];
        }
        **/
		Song song = new Song(key.toString(), songTitle);
		song.setSongHotttnesssDouble(songHotttnesss);
		song.setDanceabilityDouble(danceability);
		song.setDurationDouble(duration);
		song.setFadeInEndTime(fadeInDuration);
		song.setEnergyDouble(energy);
		song.setSongKey(songKey);
		song.setSongMode(songMode);
		song.setSongTempo(songTempo);
		song.setTimeSignatureDouble(timeSignature);
		song.setLoudnessDouble(loudness);
		song.setFadeOutStartTime(startOfFadeOut);
		song.setArtistID(artistID);
		song.setArtistName(artistName);
		
		//String[] artistTermsArray = artistTerms.trim().split("\\s+");
		ArrayList<String> uniqueTerms = new ArrayList<String>();
		
		// only add the unique artist terms for this song
		for (int i=0; i < terms.size(); i++) {
			if (!uniqueTerms.contains(terms.get(i))) {
				uniqueTerms.add(terms.get(i));
			}
		}
		
		for (String t : uniqueTerms) {
			song.addTerm(t);
		}
		
		songs.add(song);
        
        if (songHotttnesss > maxSongHotttnesss) {
        	maxSongHotttnesss = songHotttnesss;
        }
    }
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		if (songs.isEmpty()) {
			context.write(new Text("Songs is empty"), new Text("Don't know why"));
		} else {
		
		ArrayList<Song> hottestSongs = new ArrayList<Song>();
		
		songs.sort(Comparator.comparingDouble(Song::getSongHotttnesssDouble));
		Collections.reverse(songs);

		for (Song s : songs) {
			if ((s.getSongHotttnesssDouble() <= maxSongHotttnesss) && (s.getSongHotttnesssDouble() >= (maxSongHotttnesss - 0.01))) {
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
			double longestFadeOutStart = hottestSongs.stream().max(Comparator.comparing(s -> ((Song) s).getFadeOutStartTime())).get().getFadeOutStartTime();
			
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
			
			context.write(new Text("Total number of hottest Songs: "), new Text("Song size: " + hottestSongs.size()));
			//for (Song s : hottestSongs) {
				context.write(new Text("The Hottest Song Ever"), new Text("Artist: The Hotties\tdanceability: " + highestDanceability + "\tduration: " + longestDuration + "\\tfadeInEnd: " + longestFadeInTime + "\tenergy: " + highestEnergy
						 + "\tsongKey: " + highestSongKey + "\tmode: " + highestMode + "\ttempo: " + highestTempo + "\ttimeSignature: " + highestTimeSignature + "\tloudness: " + highestLoudness + "\tfadeOutStart: " + longestFadeOutStart
						 + "\tterms: " + termsString));
			//}
		}
		}

		
	}
}
