package cs455.hadoop.Util;

import java.util.ArrayList;

public class Song {

	private double danceabilityDouble;
	private double energyDouble;
	private double songKeyDouble;
	private String songTitle;
	private String songID;
	private double averageSegmentsStart;
	private double averageSegementsPitches;
	private double averageSegementsTimbre;
	private double averageSegementsLoudnessMaxTime;
	private double averageSegementsLoudnessStart;
	private double songTempo;
	private double songMode;
	private double loudnessDouble;
	private double songHotttnesssDouble;
	private double durationDouble;
	private double timeSignatureDouble;
	private double fadeInEndTime;
	private double fadeOutStartTime;
	private double fadeOutTime;
	private double totalFadeTime;
	private String artistID;
	private String artistName;
	private ArrayList<String> terms;
	private ArrayList<Double> segmentsStartList;
	private int year;
	private String cityDetails;
	private double artistHotttness;
	private double artistFamiliarity;
	private int numberOfSimilarArtists;
	
	public Song() {this.songID = null;}
	
	public Song(String songID, String songTitle) {
		this.songID = songID;
		this.songTitle = songTitle;
		this.totalFadeTime = 0;
		this.durationDouble = 0.0;
		this.danceabilityDouble = 0.0;
		this.energyDouble = 0.0;
		this.timeSignatureDouble = 0.0;
		this.segmentsStartList = new ArrayList<Double>();
		this.terms = new ArrayList<String>();
		this.songKeyDouble = 0.0;
		this.songHotttnesssDouble = 0.0;
		this.loudnessDouble = 0.0;
		this.fadeOutTime = 0.0;
	}
	
	public String getSongID() {
		return this.songID;
	}
	
	public void setSongID(String songID) {
		this.songID = songID;
	}
	
	public double getSongKey() {
		return this.songKeyDouble;
	}
	
	public void setSongKey(double d) {
		this.songKeyDouble = d;
	}
	
	public void setTimeSignatureDouble(double d) {
		this.timeSignatureDouble = d;
	}
	
	public double getTimeSignatureDouble() {
		return this.timeSignatureDouble;
	}
	
	public void setDanceabilityDouble(double d) {
		this.danceabilityDouble = d;
	}
	
	public double getDanceabilityDouble() {
		return this.danceabilityDouble;
	}
	
	public void setEnergyDouble(double d) {
		this.energyDouble = d;
	}
	
	public double getEnergyDouble() {
		return this.energyDouble;
	}
	
	public String getSongTitle() {
		return this.songTitle;
	}
	
	public void addTerm(String s) {
		this.terms.add(s);
	}
	
	public ArrayList<String> getTerms() {
		return this.terms;
	}
	
	public double getTotalFadeTime() {
		return this.totalFadeTime;
	}
	
	public void incrementTotalFadeTime(double d) {
		this.totalFadeTime += d;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Song) {
			Song otherSong = (Song) obj;
			if (this.songID.equals(otherSong.getSongID())) {
				return true;
			}
		}
	return false;
	}

	public double getAverageSegmentsStart() {
		return averageSegmentsStart;
	}

	public void setAverageSegmentsStart(double averageSegmentsStart) {
		this.averageSegmentsStart = averageSegmentsStart;
	}

	public double getAverageSegementsPitches() {
		return averageSegementsPitches;
	}

	public void setAverageSegementsPitches(double averageSegementsPitches) {
		this.averageSegementsPitches = averageSegementsPitches;
	}

	public double getAverageSegementsTimbre() {
		return averageSegementsTimbre;
	}

	public void setAverageSegementsTimbre(double averageSegementsTimbre) {
		this.averageSegementsTimbre = averageSegementsTimbre;
	}

	public double getAverageSegementsLoudnessMaxTime() {
		return averageSegementsLoudnessMaxTime;
	}

	public void setAverageSegementsLoudnessMaxTime(double averageSegementsLoudnessMaxTime) {
		this.averageSegementsLoudnessMaxTime = averageSegementsLoudnessMaxTime;
	}

	public double getAverageSegementsLoudnessStart() {
		return averageSegementsLoudnessStart;
	}

	public void setAverageSegementsLoudnessStart(double averageSegementsLoudnessStart) {
		this.averageSegementsLoudnessStart = averageSegementsLoudnessStart;
	}

	
	public double getSongTempo() {
		return songTempo;
	}

	public void setSongTempo(double songTempo) {
		this.songTempo = songTempo;
	}

	public double getDurationDouble() {
		return this.durationDouble;
	}
	
	public void setDurationDouble(double d) {
		this.durationDouble = d;
	}
	
	public double getSongMode() {
		return songMode;
	}

	public void setSongMode(double songMode) {
		this.songMode = songMode;
	}

	public double getLoudnessDouble() {
		return loudnessDouble;
	}

	public void setLoudnessDouble(double loudness) {
		this.loudnessDouble = loudness;
	}
	
	public double getSongHotttnesssDouble() {
		return this.songHotttnesssDouble;
	}
	
	public void setSongHotttnesssDouble(double d) {
		this.songHotttnesssDouble = d;
	}
	
	public double getFadeInEndTime() {
		return fadeInEndTime;
	}

	public void setFadeInEndTime(double fadeInEndTime) {
		this.fadeInEndTime = fadeInEndTime;
	}

	public double getFadeOutStartTime() {
		return fadeOutStartTime;
	}

	public void setFadeOutStartTime(double fadeOutStartTime) {
		this.fadeOutStartTime = fadeOutStartTime;
	}

	public void setSongTitle(String songTitle) {
		this.songTitle = songTitle;
	}

	public String getArtistID() {
		return artistID;
	}

	public void setArtistID(String artistID) {
		this.artistID = artistID;
	}

	public String getArtistName() {
		return artistName;
	}

	public void setArtistName(String artistName) {
		this.artistName = artistName;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public String getCityDetails() {
		return cityDetails;
	}

	public void setCityDetails(String cityDetails) {
		this.cityDetails = cityDetails;
	}

	public double getArtistHotttness() {
		return artistHotttness;
	}

	public void setArtistHotttness(double artistHotttness) {
		this.artistHotttness = artistHotttness;
	}
	
	public int getNumberOfSimilarArtists() {
		return this.numberOfSimilarArtists;
	}
	
	public void setNumberOfSimilarArtists(int n) {
		this.numberOfSimilarArtists = n;
	}
	
	public double getArtistFamiliarity() {
		return this.artistFamiliarity;
	}
	
	public void setArtistFamiliarity(double fam) {
		this.artistFamiliarity = fam;
	}
	
	public void addSegmentStart(double d) {
		this.segmentsStartList.add(d);
	}
	
	public void setFadeOutTime(double d) {
		this.fadeOutTime = d;
	}
	
	public double getFadeOutTime() {
		return this.fadeOutTime;
	}

}
