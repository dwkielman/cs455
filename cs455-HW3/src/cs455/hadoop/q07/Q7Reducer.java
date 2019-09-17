package cs455.hadoop.q07;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.hadoop.Util.Artist;
import cs455.hadoop.Util.DataUtilities;
import cs455.hadoop.Util.Song;

public class Q7Reducer extends Reducer<Text, Text, Text, Text>  {
/**
	String averageSongTitle = "";
	private long totalAverageSegmentStart = 0;
	private long totalAverageSegementsPitches = 0;
	private long totalAverageSegementsTimbre = 0;
	private long totalAverageSegmentsLoundessMaxTime = 0;
	private long totalAverageSegementsLoudnessStart = 0;
	private long numberOfStarts = 0;
	private long numberOfPitches = 0;
	private long numberOfTimbres = 0;
	private long numberOfLoudnessMaxTimes = 0;
	private long numberOfLoudnessStart = 0;
	**/
	private ArrayList<Song> songs = new ArrayList<Song>();
	private ArrayList<Double> startAverage = new ArrayList<Double>();
	private ArrayList<Double> pitchAverage = new ArrayList<Double>();
	private ArrayList<Double> timbreAverage = new ArrayList<Double>();
	private ArrayList<Double> loudnessMaxAverage = new ArrayList<Double>();
	private ArrayList<Double> loudnessStartAverage = new ArrayList<Double>();
	
	//private HashMap<Artist, ArrayList<Song>> artists = new HashMap<Artist, ArrayList<Song>>();
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String songTitle = "";
        String artistID = "";
        String artistName = "";
        double segmentsStartAverage = 0.0;
        double segmentsPitchesAverage = 0.0;
        double segmentsTimbreAverage = 0.0;
        double segmentsLoudnessMaxTimeAverage = 0.0;
        double segmentsLoudnessStartAverage = 0.0;
        
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
        		}
        	}
        }
		
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
        
        
        //Song song = new Song(key.toString(), songTitle);
		//Artist artist = new Artist(artistID, artistName);
		
        //song.setAverageSegmentsStart(segmentsStartAverage);
        //song.setAverageSegementsPitches(segmentsPitchesAverage);
        //song.setAverageSegementsTimbre(segmentsTimbreAverage);
        //song.setAverageSegementsLoudnessMaxTime(segmentsLoudnessMaxTimeAverage);
        //song.setAverageSegementsLoudnessStart(segmentsLoudnessStartAverage);
        
        //if (song.getAverageSegementsLoudnessMaxTime() > 0 && song.getAverageSegementsLoudnessStart() > 0 && song.getAverageSegementsPitches() > 0 && song.getAverageSegementsTimbre() > 0 && song.getAverageSegmentsStart() > 0) {
        	//songs.add(song);
        //}
		/**
		for (Text val : values) {
			String parts[] = val.toString().split(",");
			String songTitle = "";
			// segments start
			if (Long.parseLong(parts[0]) > 0) {
				totalAverageSegmentStart += Long.parseLong(parts[0]);
				numberOfStarts++;
			}
			
			// segments pitches
			if (Long.parseLong(parts[1]) > 0) {
				totalAverageSegementsPitches += Long.parseLong(parts[1]);
				numberOfPitches++;
			}
			
			// segments timbre
			if (Long.parseLong(parts[2]) > 0) {
				totalAverageSegementsTimbre += Long.parseLong(parts[2]);
				numberOfTimbres++;
			}
			
			// segments loudness max time
			if (Long.parseLong(parts[3]) > 0) {
				totalAverageSegmentsLoundessMaxTime += Long.parseLong(parts[3]);
				numberOfLoudnessMaxTimes++;
			}
			
			// segments loudness start
			if (Long.parseLong(parts[4]) > 0) {
				totalAverageSegementsLoudnessStart += Long.parseLong(parts[4]);
				numberOfLoudnessStart++;
			}
			songTitle = parts[5];
			Song song = new Song(key.toString(), songTitle);
			songs.add(song);
		}
		**/
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		context.write(new Text("Writing a sample of songs: "), new Text("Let's see how this goes "));
		
		if (startAverage.size() > 0) {
			double startAverageNumber = DataUtilities.getAverageValue(startAverage);
			context.write(new Text("Start Average"), new Text("Value: " + startAverageNumber));
		}
		
		if (pitchAverage.size() > 0) {
			double pitchAverageNumber = DataUtilities.getAverageValue(pitchAverage);
			context.write(new Text("Pitch Average"), new Text("Size: " + pitchAverageNumber));
		}
		
		if (timbreAverage.size() > 0) {
			double timbreAverageNumber = DataUtilities.getAverageValue(timbreAverage);
			context.write(new Text("Timbre Average"), new Text("Size: " + timbreAverageNumber));
		}
		
		if (loudnessMaxAverage.size() > 0) {
			double loudnessMaxAverageNumber = DataUtilities.getAverageValue(loudnessMaxAverage);
			context.write(new Text("Loudness Max Average"), new Text("Size: " + loudnessMaxAverageNumber));
		}
		
		if (loudnessStartAverage.size() > 0) {
			double loudnessStartAverageNumber = DataUtilities.getAverageValue(loudnessStartAverage);
			context.write(new Text("Loudness Start Average"), new Text("Size: " + loudnessStartAverageNumber));
		}
		
		/**
		float averageSegmentsStart = totalAverageSegmentStart / numberOfStarts;
		float averageSegementsPitches = totalAverageSegementsPitches / numberOfPitches;
		float averageSegementsTimbre = totalAverageSegementsTimbre / numberOfTimbres;
		float averageSegementsLoudnessMaxTime = totalAverageSegmentsLoundessMaxTime / numberOfLoudnessMaxTimes;
		float averageSegementsLoudnessStart = totalAverageSegementsLoudnessStart / numberOfLoudnessStart;
		
		ArrayList<Song> averageSongs = new ArrayList<Song>();
		
		for (Song s : songs) {
			if ((s.getAverageSegmentsStart() == averageSegmentsStart) && (s.getAverageSegementsPitches() == averageSegementsPitches) && (s.getAverageSegementsTimbre() == averageSegementsTimbre) &&
			(s.getAverageSegementsLoudnessMaxTime() == averageSegementsLoudnessMaxTime) && (s.getAverageSegementsLoudnessStart() == averageSegementsLoudnessStart)) {
				averageSongs.add(s);
			}
		}
		
		context.write(new Text("Average Song Info"), new Text("averageSegmentsStart: " + String.valueOf(averageSegmentsStart) + ", averageSegementsPitches" + String.valueOf(averageSegementsPitches) + 
				", averageSegementsTimbre: " + String.valueOf(averageSegementsTimbre) + ", averageSegementsLoudnessMaxTime: " + String.valueOf(averageSegementsLoudnessMaxTime) + ", averageSegementsLoudnessStart: " + String.valueOf(averageSegementsLoudnessStart)));
		
		for (Song s : averageSongs) {
			context.write(new Text(s.getSongTitle()), new Text(s.getSongID()));
		}
		**/
		
	}
	
	
}
