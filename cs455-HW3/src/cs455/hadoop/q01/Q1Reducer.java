package cs455.hadoop.q01;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q1Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	private long maxNumberOfSongs = -1;
	private Text artistWithMostSongs = null;
	private ArrayList<Text> artists = new ArrayList<Text>();
	private HashMap<Text, Long> artistMap = new HashMap<Text, Long>();
	
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long numberOfSongs = 0;
        String songInfo = "";
        ArrayList<String> songs = new ArrayList<String>();
        
        for (LongWritable l : values) {
        	numberOfSongs += l.get();
        }
        
        //context.write(key, new LongWritable(numberOfSongs));
        
       // String artistInfo = "";

        /**
        for (Text val : values) {
        	//String parts[] = val.toString().split(",");
        	//numberOfSongs++;
        	//artistInfo = parts[0];
        	//artists.add(val.toString());
        	//artistInfo = parts[1];
        	//songInfo+= "/t" + val.toString();
        	songs.add(val.toString());
        	
        }
        
        context.write(key, new Text("Songs: " + songs.toString()));
        **/
        
        if (artistWithMostSongs == null) {
        	maxNumberOfSongs = numberOfSongs;
        	artistWithMostSongs = key;
        } else {
        	if (numberOfSongs > maxNumberOfSongs) {
            	maxNumberOfSongs = numberOfSongs;
            	artistWithMostSongs = key;
            }
        }
        
       // context.write(key, new LongWritable(numberOfSongs));
        
        /**
        Map<Artist, Long> artistMap = new HashMap();
        
        // calculate the total number of songs
        for(Artist artist : values){
        	artistMap.put(artist, artist.getNumberOfSongs() + 1);
        }
        
        context.write(key, record);
        **/
    }
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(artistWithMostSongs, new LongWritable(maxNumberOfSongs));
	}


}
