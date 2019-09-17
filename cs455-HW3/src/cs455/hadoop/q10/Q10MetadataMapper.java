package cs455.hadoop.q10;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs455.hadoop.Util.DataUtilities;

public class Q10MetadataMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	Map<String, String> citiesData = new HashMap<String, String>(); // Stores the data from cities in format <(Latitude,Longitude), (City Name|State|Country)>
	
	
	/**
	@Override
	public void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
			@SuppressWarnings("deprecation")
			Path[] localPaths = context.getLocalCacheFiles();
		    BufferedReader citiesReader = new BufferedReader(new FileReader(localPaths[0].toString()));

		    // add the cities data to our local records
		    String cityRecord = citiesReader.readLine();
		    while ((cityRecord = citiesReader.readLine()) != null) {
		    	ArrayList<String> record = DataUtilities.dataReader(cityRecord.toString());
		    	
		    	double lat = Math.ceil(DataUtilities.doubleReader(record.get(2)));
		    	double longy = Math.ceil(DataUtilities.doubleReader(record.get(3)));
		    	
		    	BigDecimal bd = new BigDecimal(lat).setScale(1, RoundingMode.HALF_EVEN);
				double newLat = bd.doubleValue();
				
				bd = new BigDecimal(longy).setScale(1, RoundingMode.HALF_EVEN);
				double newLongy = bd.doubleValue();
				
				String latitude = String.valueOf(newLat);
		    	String longitude = String.valueOf(newLongy);
		    	String city = record.get(1);
		    	String state = record.get(7);
		    	String country = record.get(4);
		    	
		    	if (!citiesData.containsKey(latitude + "," + longitude)) {
		    		citiesData.put(latitude + "," + longitude, city + "|" + state + "|" + country);
		    	}
		    	
		    	/**
		    	String[] lineRecord = cityRecord.split(",");
		    	// may want to consider rounding to 2 decimal places at least
		    	double lat = Math.ceil(Float.parseFloat(lineRecord[2]));
		    	double longy = Math.ceil(Float.parseFloat(lineRecord[3]));
		    	
		    	BigDecimal bd = new BigDecimal(lat).setScale(1, RoundingMode.HALF_EVEN);
				double newLat = bd.doubleValue();
				
				bd = new BigDecimal(longy).setScale(1, RoundingMode.HALF_EVEN);
				double newLongy = bd.doubleValue();

		    	String latitude = String.valueOf(newLat);
		    	String longitude = String.valueOf(newLongy);
		    	String city = lineRecord[1];
		    	String state = lineRecord[7];
		    	String country = lineRecord[4];
		    	
		    	// consider putting in a different city if the population in one is higher than another
		    	if (!citiesData.containsKey(latitude + "," + longitude)) {
		    		citiesData.put(latitude + "," + longitude, city + "|" + state + "|" + country);
		    	}
		    	
		    }
		}
		
		super.setup(context);
	}
	**/
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(!value.toString().isEmpty()){
			ArrayList<String> record = DataUtilities.dataReader(value.toString());
			
			String artistID = record.get(3);
			String artistName = record.get(7);
			String songID = record.get(8);
			String songTitle = record.get(9);
			String artistHotttness = record.get(2);
			String year = record.get(14);
			String location = record.get(6);
			/**
			artistDetails = getOutsourcedCityDetails(newLatitude, newLongitude);
			
			if (artistDetails.isEmpty()) {
				artistDetails = record.get(6).replace(",", "") + "||";  
			}
			**/
			if (!songID.isEmpty() && !artistID.isEmpty() && !artistName.isEmpty() && !songTitle.isEmpty() && !artistHotttness.isEmpty() && !year.isEmpty() && !location.isEmpty()) {
				context.write(new Text(songID), new Text("metadata\t" + artistID + "," + artistName + "," + songTitle + "," + artistHotttness + "," + location + "," + year));
			}
		}
		
		
		/**
		String[] record = value.toString().split(",");
		
		if (record.length != 14) {
			return;
		}

		String songID = record[8];
		String songTitle = record[9];
		String artistHotttness = record[2]; //stays the same for the given artist
		
		double latitude = Math.ceil(Float.parseFloat(record[4]));
		double longitude = Math.ceil(Float.parseFloat(record[5]));
		
		BigDecimal bd = new BigDecimal(latitude).setScale(1, RoundingMode.HALF_EVEN);
		double newLatitude = bd.doubleValue();
		
		bd = new BigDecimal(longitude).setScale(1, RoundingMode.HALF_EVEN);
		double newLongitude = bd.doubleValue();
		
		String artistLatitude = String.valueOf(newLatitude);
		String artistLongitude = String.valueOf(newLongitude);
		
		String artistDetails = "";
		
		artistDetails = getOutsourcedCityDetails(newLatitude, newLongitude);
		
		if (artistDetails == "") {
			artistDetails = record[6].replace(",", "") + "||";  
		}
		
		String artistName = record[7];
		String year = record[14];

		context.write(new Text(songID), new Text("metadata\t" + songTitle + "," + artistHotttness + "," + artistLatitude + "," + artistLongitude + "," + artistDetails
				 + "," + artistName + "," + year));
				 **/
	}
	
	// return the String-formatted record from the outsourced data of the city name, state and country
	public String getOutsourcedCityDetails(double lat, double longy) {
		String cityDetails = "";
		
		// will try to find the closest city by comparing within 20 10th decimal places for the lat and longitude passed to this function
		int latTotal = 20;
		int longTotal = 20;
		double incrementAmount = 0.1;
		
		double startingLat = lat - 1;
		double startingLong = longy - 1;
		
		for (int i = 0; i < latTotal; i++) {
			startingLat = startingLat + (incrementAmount * i);
			for (int j = 0; j < longTotal; j++) {
				startingLong = longy + (incrementAmount * j);
				String keyFinder = String.valueOf(startingLat) + "," + String.valueOf(startingLong);
				if (citiesData.containsKey(keyFinder)) {
					cityDetails = citiesData.get(keyFinder);
					break;
				}
			}
			if (cityDetails != "") {
				break;
			}
		}
		
		return cityDetails;
	}
	

}
