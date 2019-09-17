package cs455.hadoop.q01;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Q1: Which artist has the most songs in the data set?
 * Mapper: Reads line by line, (analysis has 32 comma-delimited fields, metadata has 14 comma-delimited fields)
 * Extracts artist_id from index 3,  artist_name from index 7 and song_id from index 8 from metadata
 * Emits <"ARTIST_ID:"(artist_id), "SONG_ID:"(song_id)>, <"ARTIST_ID:"(artist_id), "ARTIST_NAME:"(artist_name)> pairs.
 */

public class Q1Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {
//public class Q1Mapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(!value.toString().isEmpty()){
			ArrayList<String> record = splitString(value.toString());
			//String[] record = splitString(value.toString());
			/**
			if (record.size() != 15) {
				return;
			}
			/**
			if (record[2].charAt(0) == '\"' && record[2].charAt(1) == 'b' && record[2].charAt(2) == '\"') {
				record[2] = record[2].substring(3);
			}
			
			if (record[2].charAt(0) == 'b' && record[2].charAt(1) == '\'') {
				record[2] = record[2].substring(1);
			}
			
			record[2] = record[2].replaceAll("\"","");
			
			record[2] = record[2].replaceAll("\'","");
**/
			
			// String artistID = record[2];
			String artistID = record.get(3);
			String songID = record.get(8);
			
			if (!artistID.isEmpty()) {
				context.write(new Text(artistID), new LongWritable(1));
			}
			//String artistName = record[6];
			//String songID = record[7];
			//context.write(new Text(artistID), new Text("1"));
		}
		
		
		
		
		
		
		//Artist artist = new Artist(artistID, artistName);
		//artist.incrementNumberOfSongs();
		
		// write each artist and their unique song id
		//context.write(artist, new Text(songID));
		
		
        // tokenize into words.
        //StringTokenizer itr = new StringTokenizer(value.toString());
        // emit word, count pairs.
        //while (itr.hasMoreTokens()) {
           // context.write(new Text(itr.nextToken()), new IntWritable(1));
        //}
    }
	
	public static ArrayList<String> splitString(String s) {
	    ArrayList<String> words = new ArrayList<String>();

	    boolean notInsideComma = true;
	    int start = 0;

	    for ( int i = 0; i < s.length() - 1; i++ )
	    {
	      if ( s.charAt( i ) == ',' && notInsideComma )
	      {
	        words.add( s.substring( start, i ) );
	        start = i + 1;
	      } else if ( s.charAt( i ) == '"' )
	        notInsideComma = !notInsideComma;
	    }

	    words.add( s.substring( start ) );

	    return words;
	  }
	
	
}
