package cs455.scaling.hash;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Computes the SHA-1 hash of a byte array, and returns its representation as a hex string
 */
public class Hash {

	public String SHA1FromBytes(byte[] data) {
		MessageDigest digest = null;
		
		try {
			digest = MessageDigest.getInstance("SHA1");
		} catch (NoSuchAlgorithmException e){
			System.out.println("Could not find algorithm SHA1: " + e);
			return null;
		}
		
		byte[] hash = digest.digest(data);
		BigInteger hashInt = new BigInteger(1, hash);
		
		return hashInt.toString(16);
	}
}
