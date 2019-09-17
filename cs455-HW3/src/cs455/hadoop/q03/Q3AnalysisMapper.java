package cs455.hadoop.q03;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Q3: What is the song with the highest hotttnesss (popularity) score?
 * Mapper: Reads line by line, (analysis has 32 comma-delimited fields, metadata has 14 comma-delimited fields)
 * Extracts song_id from index 1, song_hotttnesss from index 2 from analysis
 */

public class Q3AnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(!value.toString().isEmpty()){
			ArrayList<String> record = splitString(value.toString());
			
			String songID = record.get(1);
			String songHotttnesss = record.get(2);
			
			if (!songID.isEmpty() && !songHotttnesss.isEmpty()) {
				context.write(new Text(songID), new Text("analysis\t" + songHotttnesss));
			}
			
			
		}
		/**
		String[] record = value.toString().split(",");
		
		if (record.length != 32) {
			return;
		}
		
		String songID = record[1];
		String songHotttnesss = record[2];
		
		context.write(new Text(songID), new Text("analysis\t" + songHotttnesss));
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
