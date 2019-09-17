package cs455.hadoop.q02;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Q2: Which artists songs are the loudest on average?
 * Mapper: Reads line by line, (analysis has 32 comma-delimited fields, metadata has 14 comma-delimited fields)
 * Extracts artist_id from index 3, artist_name from index 7 and song_id from index 8 from metadata
 */
public class Q2MetadataMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(!value.toString().isEmpty()){
			ArrayList<String> record = splitString(value.toString());
			
			String artistID = record.get(3);
			String artistName = record.get(7);
			String songID = record.get(8);
			
			if (!songID.isEmpty() && !artistID.isEmpty() && !artistName.isEmpty()) {
				context.write(new Text(songID), new Text("metadata\t" + artistID + "," + artistName));
			}
			
			
			
		}
		/**
		String[] record = value.toString().split(",");
		
		if (record.length != 14) {
			return;
		}
		
		String artistID = record[3];
		String artistName = record[7];
		String songID = record[8];
		
		Artist artist = new Artist(artistID, artistName);
		artist.incrementNumberOfSongs();
		
		context.write(new Text(songID), new Text("metadata\t" + artistID + "," + artistName));
		**/
		//context.write(new Text(songID), artist);
		
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
