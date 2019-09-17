package cs455.hadoop.q09;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs455.hadoop.Util.DataUtilities;

public class Q9MetadataMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(!value.toString().isEmpty()){
			ArrayList<String> record = DataUtilities.dataReader(value.toString());
			
			String artistID = record.get(3);
			String artistName = record.get(7);
			String songID = record.get(8);
			String songTitle = record.get(9);
			String artistSongTerms = record.get(11);
			
			if (!songID.isEmpty() && !artistID.isEmpty() && !artistName.isEmpty() && !songTitle.isEmpty() && !artistSongTerms.isEmpty()) {
				context.write(new Text(songID), new Text("metadata\t" + artistID + "," + artistName + "," + songTitle + "," + artistSongTerms));
			}
		}
		/**
		String[] record = value.toString().split(",");
		
		if (record.length != 14) {
			return;
		}

		String songID = record[8];
		String songTitle = record[9];
		String artistSongTerms = record[11];

		context.write(new Text(songID), new Text("metadata\t" + songTitle + "," + artistSongTerms));
		**/
	}

}
