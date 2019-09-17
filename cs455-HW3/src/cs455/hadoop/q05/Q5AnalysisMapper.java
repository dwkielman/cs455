package cs455.hadoop.q05;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs455.hadoop.Util.DataUtilities;

/**
 * Q5: What is the longest song(s)? The shortest song(s)? The song(s) of median length?
 * Mapper: Reads line by line, (analysis has 32 comma-delimited fields, metadata has 14 comma-delimited fields)
 * Extracts song_id from index 1, duration from index 5 from analysis
 *
 */

public class Q5AnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(!value.toString().isEmpty()){
			ArrayList<String> record = DataUtilities.dataReader(value.toString());
			
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
		
		context.write(new Text(songID), new Text("analysis\t" + duration));
		**/
	}

}
