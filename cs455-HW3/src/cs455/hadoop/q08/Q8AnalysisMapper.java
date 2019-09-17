package cs455.hadoop.q08;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs455.hadoop.Util.DataUtilities;

/**
 * Q8: Which artist is the most generic? Which artist is the most unique?
 * @author Admin
 *
 */
public class Q8AnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(!value.toString().isEmpty()){
			ArrayList<String> record = DataUtilities.dataReader(value.toString());
			
			String songID = record.get(1);
			String songHotttnesss = record.get(2);
			String timeSignature = record.get(15);
			
			if (!songID.isEmpty() && !songHotttnesss.isEmpty() && !timeSignature.isEmpty()) {
				context.write(new Text(songID), new Text("analysis\t" + songHotttnesss + "," + timeSignature));
			}
		}
		
		/**
		String[] record = value.toString().split(",");
		
		if (record.length != 32) {
			return;
		}
		
		String songID = record[1];
		String songHotttnesss = record[2];
		String timeSignature = record[15];
		
		context.write(new Text(songID), new Text("analysis\t" + songHotttnesss + "," + timeSignature));
		**/
	}

}
