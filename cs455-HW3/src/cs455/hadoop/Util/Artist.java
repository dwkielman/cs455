package cs455.hadoop.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class Artist implements Writable {
	
	private String artistID;
	private String artistName;
	private long numberOfSongs;
	private ArrayList<String> terms;
	private ArrayList<Song> songs;
	private double totalTimeFading;
	private int numberOfSimilarArtists;
	private double artistFamiliarity;
	private double artistHotttnesss;
	private int numberOfGenericSongs;
	private int numberOfUniqueSongs;
	private double averageLoudness;
	
	public Artist() {}
	
	public Artist(String artistID, String artistName) {
		this.artistID = artistID;
		this.artistName = artistName;
		this.numberOfSongs = 0;
		this.totalTimeFading = 0;
		this.terms = new ArrayList<String>();
		this.songs = new ArrayList<Song>();
		this.numberOfSimilarArtists = 0;
		this.artistFamiliarity = 0.0;
		this.artistHotttnesss = 0.0;
		this.numberOfGenericSongs = 0;
		this.numberOfUniqueSongs = 0;
		this.averageLoudness = 0.0;
	}
	
	public void incrementNumberOfSongs() {
		this.numberOfSongs++;
	}

	public long getNumberOfSongs() {
		return this.numberOfSongs;
	}
	
	public int getNumberOfGenericSongs() {
		return this.numberOfGenericSongs;
	}
	
	public void setNumberOfGenericsSongs(int n) {
		this.numberOfGenericSongs = n;
	}
	
	public void setNumberOfUniqueSongs(int n) {
		this.numberOfUniqueSongs = n;
	}
	
	public void incrementNumberOfGenericSongs() {
		this.numberOfGenericSongs++;
	}
	
	public int getNumberOfUniqueSongs() {
		return this.numberOfUniqueSongs;
	}
	
	public void incrementNumberOfUniqueSongs() {
		this.numberOfUniqueSongs++;
	}
	
	public void setArtistHotttnesss(double d) {
		this.artistHotttnesss = d;
	}
	
	public double getArtistHotttnesss() {
		return this.artistHotttnesss;
	}
	
	public void setArtistFamiliarity(double d) {
		this.artistFamiliarity = d;
	}
	
	public double getArtistFamiliarity() {
		return this.artistFamiliarity;
	}
	
	public void setNumberOfSimilarArtists(int n) {
		this.numberOfSimilarArtists = n;
	}
	
	public int getNumberOfSimilarArtists() {
		return this.numberOfSimilarArtists;
	}
	
	public void addSong(Song s) {
		this.songs.add(s);
	}
	
	public ArrayList<Song> getSongs() {
		return this.songs;
	}
	
	public double getTotalTimeFading() {
		return this.totalTimeFading;
	}
	
	public void incrementTotalTimeFading(double d) {
		this.totalTimeFading+=d;
	}
	
	public String getArtistID() {
		return this.artistID;
	}
	
	public String getArtistName() {
		return this.artistName;
	}
	
	public void addTerm(String s) {
		this.terms.add(s);
	}
	
	public void setAverageLoudness(double d) {
		this.averageLoudness = d;
	}
	
	public double getAverageLoudness() {
		return this.averageLoudness;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Artist) {
			Artist other = (Artist) obj;
			if (artistID == null) {
				if (other.artistID != null)
					return false;
			} else if (artistID.equals(other.artistID))
				return true;
		}
		return false;
	}
	
	@Override
	public String toString() {
		return ("Artist Name: " + this.artistName + ", Artist ID: " + this.artistID);
	}

	@Override
	public void readFields(DataInput datainput) throws IOException {
		artistID = datainput.readLine();
		artistName = datainput.readLine();
	}

	@Override
	public void write(DataOutput dataoutput) throws IOException {
		dataoutput.writeChars(artistID);
		dataoutput.writeChars(artistName);
	}
	
}
