package cs455.hadoop.q04;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Q4: Which artist has the highest total time spent fading in their songs?
 * Mapper: Reads line by line, (analysis has 32 comma-delimited fields, metadata has 14 comma-delimited fields)
 * Extracts song_id from index 1, duration from index 5, end_of_fade_in from index 6, start_of_fade_out from index 13 from analysis
 */

public class Q4AnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(!value.toString().isEmpty()){
			ArrayList<String> record = splitString(value.toString());
			
			String songID = record.get(1);
			String duration = record.get(5);
			String endOfFadeIn = record.get(6);
			String startOfFadeOut = record.get(13);
			
			if (!songID.isEmpty() && !duration.isEmpty() && !endOfFadeIn.isEmpty() && !startOfFadeOut.isEmpty()) {
				context.write(new Text(songID), new Text("analysis\t" + duration + "," + endOfFadeIn + "," + startOfFadeOut));
			}
		}
		
		/**
		String[] record = value.toString().split(",");
		
		if (record.length != 32) {
			return;
		}
		
		String songID = record[1];
		String duration = record[5];
		String endOfFadeIn = record[6];
		String startOfFadeOut = record[13];
		
		context.write(new Text(songID), new Text("analysis\t" + duration + "," + endOfFadeIn + "," + startOfFadeOut));
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
