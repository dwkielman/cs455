package cs455.hadoop.q02;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Q2: Which artists songs are the loudest on average?
 * Mapper: Reads line by line, (analysis has 32 comma-delimited fields, metadata has 14 comma-delimited fields)
 * Extracts song_id from index 1, loudness from index 10 from analysis
 */

public class Q2AnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(!value.toString().isEmpty()){
			ArrayList<String> record = splitString(value.toString());
			
			String songID = record.get(1);
			String loudness = record.get(10);
			
			if (!songID.isEmpty() && !loudness.isEmpty()) {
				context.write(new Text(songID), new Text("analysis\t" + loudness));
			}
			
			
		}
		
		/**
		String[] record = value.toString().split(",");
		
		/**
		if (record.length != 32) {
			return;
		}
		**/
		/**
		String songID = record[1];
		String loudness = record[10];
		
		context.write(new Text(songID), new Text("analysis\t" + loudness));
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
