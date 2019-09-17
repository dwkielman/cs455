package cs455.hadoop.q03;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Q3: What is the song with the highest hotttnesss (popularity) score?
 * Mapper: Reads line by line, (analysis has 32 comma-delimited fields, metadata has 14 comma-delimited fields)
 * Extracts song_id from index 8 and title from index 9 from metadata
 *
 */

public class Q3MetadataMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(!value.toString().isEmpty()){
			ArrayList<String> record = splitString(value.toString());
			
			String artistID = record.get(3);
			String artistName = record.get(7);
			String songID = record.get(8);
			String songTitle = record.get(9);
			
			if (!songID.isEmpty() && !artistID.isEmpty() && !artistName.isEmpty() && !songTitle.isEmpty()) {
				context.write(new Text(songID), new Text("metadata\t" + artistID + "," + artistName + "," + songTitle));
			}
			
			
			
		}
		
		/**
		String[] record = value.toString().split(",");
		
		if (record.length != 14) {
			return;
		}

		String songID = record[8];
		String songTitle = record[9];

		context.write(new Text(songID), new Text("metadata\t" + songTitle));
		**/
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
