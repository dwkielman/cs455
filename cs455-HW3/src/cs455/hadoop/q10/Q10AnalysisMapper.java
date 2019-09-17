package cs455.hadoop.q10;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs455.hadoop.Util.DataUtilities;

/**
 * Q10: Come up with an interesting question of your own to answer. This question should be more complex than Q7, Q8 or Q9. Answer it.
 * For this component, think of yourself as the lead data scientist at a start-up firm. What would do with this dataset that is cool?
 * You are allowed to: (1) combine your analysis with other datasets, (2) use other frameworks
 * @author Admin
 *
 */
public class Q10AnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(!value.toString().isEmpty()){
			ArrayList<String> record = DataUtilities.dataReader(value.toString());
			
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

}
