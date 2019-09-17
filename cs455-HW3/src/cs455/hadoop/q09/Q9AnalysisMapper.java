package cs455.hadoop.q09;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs455.hadoop.Util.DataUtilities;

/**
 * Q9: Imagine a song with a higher hotttnesss score than the song in your answer to Q3. List this songs tempo, 
 * time signature, danceability, duration, mode, energy, key, loudness, when it stops fading in, when it starts 
 * fading out, and which terms describe the artist who made it. Give both the song and the artist who made it unique names.
 * @author Admin
 *
 */
public class Q9AnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(!value.toString().isEmpty()){
			ArrayList<String> record = DataUtilities.dataReader(value.toString());
			
			String songID = record.get(1);
			String songHotttnesss = record.get(2);
			String danceability = record.get(4);
			String duration = record.get(5);
			String endOfFadeIn = record.get(6);
			String energy = record.get(7);
			String songKey = record.get(8);
			String mode = record.get(11);
			String tempo = record.get(14);
			String timeSignature = record.get(15);
			String loudness = record.get(10);
			String startOfFadeOut = record.get(13);
			
			if (!songID.isEmpty() && !duration.isEmpty() && !endOfFadeIn.isEmpty() && !startOfFadeOut.isEmpty() && !songHotttnesss.isEmpty() && !danceability.isEmpty() && !energy.isEmpty() && !songKey.isEmpty() && !mode.isEmpty() && !tempo.isEmpty() && !timeSignature.isEmpty() && !loudness.isEmpty()) {
				context.write(new Text(songID), new Text("analysis\t" + duration + "," + endOfFadeIn + "," + startOfFadeOut + "," + songHotttnesss + "," + danceability + "," + energy
						 + "," + songKey + "," + mode + "," + tempo + "," + timeSignature + "," + loudness));
			}
		}
		/**
		String[] record = value.toString().split(",");
		
		if (record.length != 32) {
			return;
		}
		
		String songID = record[1];
		String songHotttnesss = record[2];
		String danceability = record[4];
		String duration = record[5];
		String fadeInEnd = record[6];
		String energy = record[7];
		String songKey = record[8];
		String mode = record[11];
		String tempo = record[14];
		String timeSignature = record[15];
		String loudness = record[10];
		String fadeOutStart = record[13];
		
		context.write(new Text(songID), new Text("analysis\t" + songHotttnesss + "," + danceability + "," + duration + "," + fadeInEnd + "," + energy + "," + songKey + "," + mode + "," + tempo
				 + "," + timeSignature + "," + loudness + "," + fadeOutStart));
				 **/
	}
}
