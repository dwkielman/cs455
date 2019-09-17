package cs455.hadoop.q07;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringJoiner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import cs455.hadoop.Util.DataUtilities;

public class Q7Combiner extends Reducer<Text, Text, Text, Text>  {

	private final StringBuilder sb = new StringBuilder();
	private final Text output = new Text();
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text val : values) {
        	String parts[] = val.toString().split("\t");
        	if (parts[0].equals("analysis")) {
        		String[] record = parts[1].split(",");
        		ArrayList<Double> data = new ArrayList<Double>();
        		double average = 0.0;
        		String cell = record[0];
        		data = DataUtilities.segmentsMaker(cell);
        		average = DataUtilities.getAverageValue(data);
        		sb.append("SEGMENTSSTARTAVERAGE_" + average);
        		sb.append(",");
        		
        		data = new ArrayList<Double>();
        		average = 0.0;
        		
        		data = DataUtilities.segmentsMaker(record[1]);
        		average = DataUtilities.getAverageValue(data);
        		sb.append("SEGMENTSPITCHESAVERAGE_" + average);
        		sb.append(",");
        		
        		data = new ArrayList<Double>();
        		average = 0.0;
        	
        		data = DataUtilities.segmentsMaker(record[2]);
        		average = DataUtilities.getAverageValue(data);
        		sb.append("SEGMENTSTIMBREAVERAGE_" + average);
        		sb.append(",");
        		
        		data = new ArrayList<Double>();
        		average = 0.0;
        		
        		data = DataUtilities.segmentsMaker(record[3]);
        		average = DataUtilities.getAverageValue(data);
        		sb.append("SEGMENTSLOUDNESSMAXTIMEAVERAGE_" + average);
        		sb.append(",");
        		
        		data = new ArrayList<Double>();
        		average = 0.0;
        		
        		data = DataUtilities.segmentsMaker(record[4]);
        		average = DataUtilities.getAverageValue(data);
        		sb.append("SEGMENTSLOUDNESSSTARTAVERAGE_" + average);
        		sb.append(",");

        		output.set(sb.toString());
        		sb.setLength(0);
        		sb.trimToSize();
        		context.write(key, output);

        	} else if (parts[0].equals("metadata")) {
        		String[] record = parts[1].split(",");

        		sb.append("ARTISTID_" + record[0]);
        		sb.append(",");
        		sb.append("ARTISTNAME_" + record[1]);
        		sb.append(",");
        		sb.append("SONGTITLE_" + record[2]);
        		sb.append(",");
        		
        		output.set(sb.toString());
        		sb.setLength(0);
        		sb.trimToSize();
        		context.write(key, output);

        	}
        }
    }
}
