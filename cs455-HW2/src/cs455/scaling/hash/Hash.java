package cs455.scaling.hash;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

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
	/**
	public static void main(String[] args) {

		byte[] messageBytes = createRandomBytes();
		String message = null;
		
		Hash hash = new Hash();

		message = hash.SHA1FromBytes(messageBytes);

		ByteBuffer buffer = ByteBuffer.wrap(messageBytes);
		buffer.rewind();

		System.out.println(message);
		
		}
	
	private final static int bufferSize = 8192;
	
	public static byte[] createRandomBytes() {
		Random random = new Random();
		byte[] randomBytes = new byte[bufferSize];
		random.nextBytes(randomBytes);
		
		return randomBytes;
	}
	**/
}
