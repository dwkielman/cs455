package cs455.hadoop.q09;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.hadoop.Util.DataUtilities;

public class Q9Combiner extends Reducer<Text, Text, Text, Text>  {

	private final StringBuilder sb = new StringBuilder();
	private final Text output = new Text();
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		for (Text val : values) {
        	String parts[] = val.toString().split("\t");
        	if (parts[0].equals("analysis")) {
        		String[] record = parts[1].split(",");

        		sb.append("DURATION_#_" + record[0]);
        		sb.append(",");
        		sb.append("ENDOFFADEIN_#_" + record[1]);
        		sb.append(",");
        		sb.append("STARTOFFADEOUT_#_" + record[2]);
        		sb.append(",");
        		sb.append("SONGHOTTTNESSS_#_" + record[3]);
        		sb.append(",");
        		sb.append("DANCEABILITY_#_" + record[4]);
        		sb.append(",");
        		sb.append("ENERGY_#_" + record[5]);
        		sb.append(",");
        		sb.append("SONGKEY_#_" + record[6]);
        		sb.append(",");
        		sb.append("SONGMODE_#_" + record[7]);
        		sb.append(",");
        		sb.append("TEMPO_#_" + record[8]);
        		sb.append(",");
        		sb.append("TIMESIGNATURE_#_" + record[9]);
        		sb.append(",");
        		sb.append("LOUDNESS_#_" + record[10]);
        		sb.append(",");

        		output.set(sb.toString());
        		sb.setLength(0);
        		sb.trimToSize();
        		context.write(key, output);
        	} else if (parts[0].equals("metadata")) {
        		String[] record = parts[1].split(",");
        		sb.append("ARTISTID_#_" + record[0]);
        		sb.append(",");
        		sb.append("ARTISTNAME_#_" + record[1]);
        		sb.append(",");
        		sb.append("SONGTITLE_#_" + record[2]);
        		sb.append(",");
        		sb.append("SONGTERMS_#_" + record[3]);
        		sb.append(",");
        		
        		output.set(sb.toString());
        		sb.setLength(0);
        		sb.trimToSize();
        		context.write(key, output);
        	}
        }
		/**
		String songInfo = "";
		String songHotttnesss = "";
		String danceability = "";
		String duration = "";
		String fadeInEnd = "";
		String energy = "";
		String songKey = "";
		String mode = "";
		String tempo = "";
		String timeSignature = "";
		String loudness = "";
		String fadeOutStart = "";
		String artistTerms = "";
        
        for (Text val : values) {
        	String parts[] = val.toString().split("\t");
        	if (parts[0].equals("analysis")) {
        		String records[] = parts[1].split(",");
        		songHotttnesss = records[0];
        		danceability = records[1];
        		duration = records[2];
        		fadeInEnd = records[3];
        		energy = records[4];
        		songKey = records[5];
        		mode = records[6];
        		tempo = records[7];
        		timeSignature = records[8];
        		loudness = records[9];
        		fadeOutStart = records[10];
        	} else if (parts[0].equals("metadata")) {
        		String records[] = parts[1].split(",");
        		songInfo = records[0];
        		artistTerms = records[1];
        	}
        }
        
        context.write(key, new Text(songHotttnesss + "," + danceability + "," + duration + "," + fadeInEnd + "," + energy + "," + songKey + "," + mode + "," + tempo + "," + timeSignature + "," + loudness
        		 + "," + fadeOutStart + "," + songInfo + "," + artistTerms));
        		 **/
    }

}
