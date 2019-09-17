package cs455.hadoop.q08;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs455.hadoop.Util.DataUtilities;

public class Q8MetadataMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(!value.toString().isEmpty()){
			ArrayList<String> record = DataUtilities.dataReader(value.toString());

			String artistID = record.get(3);
			String artistName = record.get(7);
			String songID = record.get(8);
			String songTitle = record.get(9);
			String artistFamiliarity = record.get(1);
			String artistHotttnesss = record.get(2);
			String similarArtists = record.get(10);
			
			if (!songID.isEmpty() && !artistID.isEmpty() && !artistName.isEmpty() && !songTitle.isEmpty() && !artistFamiliarity.isEmpty() && !artistHotttnesss.isEmpty() && !similarArtists.isEmpty()) {
				context.write(new Text(songID), new Text("metadata\t" + artistID + "," + artistName + "," + songTitle + "," + artistFamiliarity + "," + artistHotttnesss + "," + similarArtists));
			}
		}
		/**
		String[] record = value.toString().split(",");
		
		if (record.length != 14) {
			return;
		}

		String songID = record[8];
		String artistID = record[3];
		String artistName = record[7];
		String artistFamiliarity = record[1];
		String artistHotttnesss = record[2];
		String songTitle = record[9];
		String similarArtists = record[10];

		context.write(new Text(songID), new Text("metadata\t" + artistID + "," + artistName + "," + songTitle + "," + artistFamiliarity + "," + artistHotttnesss + "," + similarArtists));
		**/
	}

}
