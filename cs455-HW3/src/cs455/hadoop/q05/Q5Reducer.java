package cs455.hadoop.q05;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import cs455.hadoop.Util.Artist;
import cs455.hadoop.Util.DataUtilities;
import cs455.hadoop.Util.Song;

public class Q5Reducer extends Reducer<Text, Text, Text, Text>  {

	/**
	private MultipleOutputs multipleOutputs;
	private float maxSongDuration = -1;
	private float minSongDuration = -1;
	private float averageSongLength = -1;
	private long totalSongLength = 0;
	private long numberOfSongs = 0;
	private Map<Text, Text> songsMap;
	private Map<Text, Text> q5LongestSongs;
	private Map<Text, Text> q5ShortestSongs;
	private Map<Text, Text> q5AverageSongs;
	**/
	private ArrayList<Song> songs = new ArrayList<Song>();
	private HashMap<Artist, ArrayList<Song>> artists = new HashMap<Artist, ArrayList<Song>>();
	
	/**
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		q5LongestSongs = new HashMap<Text, Text>();
		q5ShortestSongs = new HashMap<Text, Text>();
		q5AverageSongs = new HashMap<Text, Text>();
		songsMap = new HashMap<Text, Text>();
		multipleOutputs = new MultipleOutputs(context);
	}
	**/
	
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
        			duration = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("ENDOFFADEIN")) {
        			fadeInDuration = DataUtilities.doubleReader(parts[1]);
        		} else if (parts[0].equals("STARTOFFADEOUT")) {
        			startOfFadeOut = DataUtilities.doubleReader(parts[1]);
        		}
        	}
        }
        
		Song song = new Song(key.toString(), songTitle);
		Artist artist = new Artist(artistID, artistName);
		
		double fadeOutDuration = duration - startOfFadeOut;
		totalFadeTime = fadeInDuration + fadeOutDuration;
		song.incrementTotalFadeTime(totalFadeTime);
		song.setDurationDouble(duration);
		songs.add(song);
		
		/**
        float duration = 0;
        String songTitle = "";
        
        for (Text val : values) {
        	String parts[] = val.toString().split(",");
        	duration = Float.parseFloat(parts[0]);
        	songTitle = parts[1];
        	totalSongLength += duration;
        	numberOfSongs++;
        	songsMap.put(key, val);
        }

        if (duration > maxSongDuration) {
        	maxSongDuration = duration;
        }
        
        // get the shortest song, initialize to whatever the first song's duration is
        if (minSongDuration == -1) {
        	minSongDuration = duration;
        } else if (duration < minSongDuration) {
        	minSongDuration = duration;
        }
        **/
    }
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		double maxSongDuration = 0;
		double minSongDuration = Double.MAX_VALUE;
		double averageSongDuration = 0;
		double totalSongDuration = 0;
		int numberOfSongs = 0;
		
		
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
				numberOfSongs++;
			}
		}
		
		if (numberOfSongs > 0) {
			averageSongDuration = totalSongDuration / numberOfSongs;
		}
		
		ArrayList<Song> longestDurationSongsList = new ArrayList<Song>();
		ArrayList<Song> shortestDurationSongsList = new ArrayList<Song>();
		ArrayList<Song> averageDurationSongsList = new ArrayList<Song>();
		
		for (Song s : songs) {
			if (s.getDurationDouble() == maxSongDuration) {
				longestDurationSongsList.add(s);
			} else if (s.getDurationDouble() == minSongDuration) {
				shortestDurationSongsList.add(s);
			} else if (DataUtilities.roundDouble(s.getDurationDouble(), 2) == DataUtilities.roundDouble(averageSongDuration, 2)) {
				averageDurationSongsList.add(s);
			}
		}
		
		context.write(new Text("Longest Songs: "), new Text("We have this many songs that are the longest " + longestDurationSongsList.size()));
		
		int songCounter = 3;
		
		if (longestDurationSongsList.size() > 0) {
			if (longestDurationSongsList.size() < 3) {
				songCounter = longestDurationSongsList.size();
			}
			for (int i=0; i < songCounter; i++) {
				context.write(new Text(longestDurationSongsList.get(i).getSongTitle()), new Text("Is of duratrion: " + longestDurationSongsList.get(i).getDurationDouble()));
			}
		}
		
		context.write(new Text("Shortest Songs: "), new Text("We have this many songs that are the shortest " + shortestDurationSongsList.size()));
		
		songCounter = 3;
		
		if (shortestDurationSongsList.size() > 0) {
			if (shortestDurationSongsList.size() < 3) {
				songCounter = shortestDurationSongsList.size();
			}
			for (int i=0; i < songCounter; i++) {
				context.write(new Text(shortestDurationSongsList.get(i).getSongTitle()), new Text("Is of duratrion: " + shortestDurationSongsList.get(i).getDurationDouble()));
			}
		}
		
		context.write(new Text("Average Songs: "), new Text("We have this many songs that are the average " + averageDurationSongsList.size()));
		
		songCounter = 3;
		
		if (averageDurationSongsList.size() > 0) {
			if (averageDurationSongsList.size() < 3) {
				songCounter = averageDurationSongsList.size();
			}
			for (int i=0; i < songCounter; i++) {
				context.write(new Text(averageDurationSongsList.get(i).getSongTitle()), new Text("Is of duratrion: " + averageDurationSongsList.get(i).getDurationDouble()));
			}
		}
		
		/**
		averageSongLength = totalSongLength / numberOfSongs;
		
		Iterator<Entry<Text, Text>> it = songsMap.entrySet().iterator();
	    while (it.hasNext()) {
	    	float duration = 0;
	        String songTitle = "";
	        
	        Map.Entry pair = (Map.Entry)it.next();
	        String parts[] = pair.getValue().toString().split(",");
	        duration = Float.parseFloat(parts[0]);
        	songTitle = parts[1];
	        if (duration == maxSongDuration) {
	        	q5LongestSongs.put(new Text(songTitle), new Text(String.valueOf(duration)));
	        } else if (duration == averageSongLength) {
	        	q5AverageSongs.put(new Text(songTitle), new Text(String.valueOf(duration)));
	        } else if (duration == minSongDuration) {
	        	q5ShortestSongs.put(new Text(songTitle), new Text(String.valueOf(duration)));
	        }
	        it.remove(); // avoids a ConcurrentModificationException
	    }
	    
	    for (Text key : q5LongestSongs.keySet()) {
	    	multipleOutputs.write(key, q5LongestSongs.get(key), "/home/HW3/Q5Outputs/LongestSongs");
	    }
	    
	    for (Text key : q5AverageSongs.keySet()) {
	    	multipleOutputs.write(key, q5AverageSongs.get(key), "/home/HW3/Q5Outputs/AverageSongs");
	    }
	    
	    for (Text key : q5ShortestSongs.keySet()) {
	    	multipleOutputs.write(key, q5ShortestSongs.get(key), "/home/HW3/Q5Outputs/ShortestSongs");
	    }
		
	    multipleOutputs.close();
	    super.cleanup(context);
		**/
	}
}
