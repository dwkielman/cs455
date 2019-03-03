package cs455.scaling.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import cs455.scaling.hash.Hash;

/**
 * The client is expected to send messages at the rate specified during start-up. The client sends a byte[] to the server. The size of this array is 8 KB and the contents of this array are randomly generated. The
 * client generates a new byte array for every transmission and also tracks the hash codes associated with the data that it transmits. Hashes will be generated with the SHA-1 algorithm
 */

public class SenderThread implements Runnable {

	private final int messageDividend = 1000;
	private final int bufferSize = 8192;
	private final int messageRate;
	private final ClientStatistics clientStatistics;
	private final SocketChannel socketChannel;
	private final Hash hash = new Hash();
	
	public SenderThread(SocketChannel socketChannel, int messageRate, ClientStatistics clientStatistics) {
		this.socketChannel = socketChannel;
		this.messageRate = messageDividend / messageRate;
		this.clientStatistics = clientStatistics;
	}
	
	@Override
	public void run() {
		byte[] messageBytes = createRandomBytes();
		String message = null;
		
		while(true) {
			message = hash.SHA1FromBytes(messageBytes);

			ByteBuffer buffer = ByteBuffer.wrap(messageBytes);
			buffer.rewind();

			// synchronize?
			
			// this may be incorrect, need to test and figure out
			try {
				socketChannel.write(buffer);
				buffer.clear();
				//buffer.put(messageBytes);
				//buffer.flip();
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
			
			// increment the send message here
			this.clientStatistics.incrementMessagesSent();
			// add the message to the hash code tracker
			this.clientStatistics.addHashCode(message);
			
			// sleep until more messages are ready to be sent
			try {
				Thread.sleep(messageRate);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}

	private byte[] createRandomBytes() {
		byte[] randomBytes = new byte[bufferSize];
		new Random().nextBytes(randomBytes);
		
		return randomBytes;
	}

}
