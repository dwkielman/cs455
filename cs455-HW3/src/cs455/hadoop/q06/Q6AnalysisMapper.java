package cs455.hadoop.q06;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs455.hadoop.Util.DataUtilities;

/**
 * Q6: What are the 10 most energetic and danceable songs? List them in descending order.
 * Mapper: Reads line by line, (analysis has 32 comma-delimited fields, metadata has 14 comma-delimited fields)
 * Extracts song_id from index 1, danceability from index 4, energy from index 7 from analysis
 *
 */
public class Q6AnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(!value.toString().isEmpty()){
			ArrayList<String> record = DataUtilities.dataReader(value.toString());
			
			String songID = record.get(1);
			String danceability = record.get(4);
			String energy = record.get(7);
			
			if (!songID.isEmpty() && !danceability.isEmpty() && !energy.isEmpty()) {
				context.write(new Text(songID), new Text("analysis\t" + danceability + "," + energy));
			}
		}
		/**
		String[] record = value.toString().split(",");
		
		if (record.length != 32) {
			return;
		}
		
		String songID = record[1];
		String danceability = record[4];
		String energy = record[7];
		
		context.write(new Text(songID), new Text("analysis\t" + danceability + "," + energy));
		**/
	}

}
