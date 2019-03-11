package cs455.scaling.client;

import java.io.DataOutputStream;
import java.util.Random;

import cs455.scaling.hash.Hash;

/**
 * The client is expected to send messages at the rate specified during start-up. The client sends a byte[] to the server. The size of this array is 8 KB and the contents of this array are randomly generated. The
 * client generates a new byte array for every transmission and also tracks the hash codes associated with the data that it transmits. Hashes will be generated with the SHA-1 algorithm
 */

public class SenderThread implements Runnable {

	private static final int MESSAGE_DIVIDEND = 1000;
	private static final int BUFFER_SIZE = 8192;
	private int messageRate;
	private ClientStatistics clientStatistics;
	private Hash hash = new Hash();
	private DataOutputStream dataOutputStream;

	public SenderThread(DataOutputStream dataOutputStream, int messageRate, ClientStatistics clientStatistics) {
		this.dataOutputStream = dataOutputStream;
		this.messageRate = MESSAGE_DIVIDEND / messageRate;
		this.clientStatistics = clientStatistics;
	}
	
	// When an acknowledgement is received from the server, the client checks the hashcode in the acknowledgement by scanning through the linked list
	public void readHash(String hashedString) {
		this.clientStatistics.incrementMessagesReceived();
		// Once the hashcode has been verified, it can be removed from the linked list
		if (this.clientStatistics.containsHashCode(hashedString.trim())) {
			this.clientStatistics.removeHashCode(hashedString.trim());
		} else {
			System.out.println("Incorrect hash message sent to Client");
		}
	}
	
	@Override
	public void run() {
		while (true) {
			byte[] messageBytes = createRandomBytes();
			try {
				String message = hash.SHA1FromBytes(messageBytes);
				Thread.sleep(messageRate);
				this.dataOutputStream.write(messageBytes);
				
				// For every data packet that is published, the client adds the corresponding hashcode to the linked list
				this.clientStatistics.addHashCode(message);
				this.clientStatistics.incrementMessagesSent();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	// generates a message of random bytes for sending
	private byte[] createRandomBytes() {
		Random random = new Random();
		byte[] randomBytes = new byte[BUFFER_SIZE];
		random.nextBytes(randomBytes);
		
		return randomBytes;
	}

}
