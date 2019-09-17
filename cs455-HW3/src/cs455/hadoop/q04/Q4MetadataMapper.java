package cs455.hadoop.q04;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Q4: Which artist has the highest total time spent fading in their songs?
 * Mapper: Reads line by line, (analysis has 32 comma-delimited fields, metadata has 14 comma-delimited fields)
 * Extracts artist_name from index 7, song_id from index 8 from metadata
 *
 */

public class Q4MetadataMapper extends Mapper<LongWritable, Text, Text, Text> {
	
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

		String artistName = record[7];
		String songID = record[8];

		context.write(new Text(songID), new Text("metadata\t" + artistName));
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
