package cs455.hadoop.q07;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs455.hadoop.Util.DataUtilities;

/**
 * Q7: Create segment data for the average song. Include start time, pitch, timbre, max loudness, max loudness time, and start loudness.
 * Mapper: Reads line by line, (analysis has 32 comma-delimited fields, metadata has 14 comma-delimited fields)
 * Extracts song_id from index 1, segments_start from index 18, segments_pitches from index 20, segments_timbre from index 21, segments_loudness_max_time from index 23,
 * segments_loudness_start from index 24 from analysis
 */

public class Q7AnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(!value.toString().isEmpty()){
			ArrayList<String> record = DataUtilities.dataReader(value.toString());
			
			String songID = record.get(1);
			String segmentsStart = record.get(18);
			String segementsPitches = record.get(20);
			String segementsTimbre = record.get(21);
			String segementsLoudnessMaxTime = record.get(23);
			String segementsLoudnessStart = record.get(24);
			
			if (!songID.isEmpty() && !segmentsStart.isEmpty() && !segementsPitches.isEmpty() && !segementsTimbre.isEmpty() && !segementsLoudnessMaxTime.isEmpty() && !segementsLoudnessStart.isEmpty()) {
				context.write(new Text(songID), new Text("analysis\t" + segmentsStart + "," + segementsPitches + "," + segementsTimbre + "," + segementsLoudnessMaxTime + "," + segementsLoudnessStart));
			}
		}
		/**
		String[] record = value.toString().split(",");
		
		if (record.length != 32) {
			return;
		}
		
		String songID = record[1];
		String segmentsStart = record[18];
		String segementsPitches = record[20];
		String segementsTimbre = record[21];
		String segementsLoudnessMaxTime = record[23];
		String segementsLoudnessStart = record[24];
		
		String averageSegmentStart = getAverageValue(segmentsStart);
		String averageSegementsPitches = getAverageValue(segementsPitches);
		String averageSegementsTimbre = getAverageValue(segementsTimbre);
		String averageSegementsLoudnessMaxTime = getAverageValue(segementsLoudnessMaxTime);
		String averageSegementsLoudnessStart = getAverageValue(segementsLoudnessStart);
		
		context.write(new Text(songID), new Text("analysis\t" + averageSegmentStart + "," + averageSegementsPitches + "," + averageSegementsTimbre + "," + averageSegementsLoudnessMaxTime + "," + averageSegementsLoudnessStart));
		**/
		
	}
}
