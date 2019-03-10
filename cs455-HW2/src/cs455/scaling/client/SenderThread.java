package cs455.scaling.client;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
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
	private SocketChannel socketChannel;
	private Hash hash = new Hash();
	private DataOutputStream dataOutputStream;
	private SelectionKey key;
	
	public SenderThread(SocketChannel socketChannel, int messageRate, ClientStatistics clientStatistics, SelectionKey key) {
		this.socketChannel = socketChannel;
		this.messageRate = MESSAGE_DIVIDEND / messageRate;
		this.clientStatistics = clientStatistics;
		this.key = key;
	}
	
	public SenderThread(DataOutputStream dataOutputStream, int messageRate, ClientStatistics clientStatistics) {
		this.dataOutputStream = dataOutputStream;
		this.messageRate = MESSAGE_DIVIDEND / messageRate;
		this.clientStatistics = clientStatistics;
	}
	
	public void readHash(String hashedString) {
		//System.out.println("Reading data from server...");
		this.clientStatistics.incrementMessagesReceived();
		if (this.clientStatistics.containsHashCode(hashedString.trim())) {
			this.clientStatistics.removeHashCode(hashedString.trim());
			//System.out.println("Removed Task with Hashed String " + hashedString);
		} else {
			System.out.println("Incorrect work sent to Client");
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
				this.clientStatistics.addHashCode(message);
				this.clientStatistics.incrementMessagesSent();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	public void run() {
		SocketChannel socketChannel = (SocketChannel) key.channel();

		while(true) {
			// testing wrapping this in sending a message only when the key is in writing mode
			byte[] messageBytes = createRandomBytes();
			String message = null;
			
			message = hash.SHA1FromBytes(messageBytes);

			ByteBuffer buffer = ByteBuffer.wrap(messageBytes);
			buffer.rewind();

			while (buffer.hasRemaining()) {
				try {
					socketChannel.write(buffer);
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
			}
			
			// increment the send message here
			this.clientStatistics.incrementMessagesSent();
			// add the message to the hash code tracker
			this.clientStatistics.addHashCode(message);
			
			buffer.clear();
			// After writing, register an interest in Reading.
	        // temporarily commenting this out
			//this.key.interestOps(SelectionKey.OP_READ);
			//this.key.interestOps(SelectionKey.OP_WRITE);
			// sleep until more messages are ready to be sent
			System.out.println("Message Sent: " + message);
			try {
				Thread.sleep(messageRate);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		//}
	}
**/
	private byte[] createRandomBytes() {
		Random random = new Random();
		byte[] randomBytes = new byte[BUFFER_SIZE];
		random.nextBytes(randomBytes);
		
		return randomBytes;
	}

}
