package cs455.overlay.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The registry informs nodes in the overlay when they should start sending messages to each other.
 * Message Type (int): TASK_INITIATE (6006)
 * Rounds (int): X
 */

public class TaskInitiate implements Event {

	private final int type = Protocol.TASK_INITIATE;
	private int numberOfRounds;
	
	public TaskInitiate(int rounds) {
		this.numberOfRounds = rounds;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * numberOfRounds
	 * @throws IOException 
	 */
	public TaskInitiate(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.TASK_INITIATE) {
			System.out.println("Invalid Message Type for TaskInitiate");
			return;
		}
		
		int numberOfRounds = din.readInt();

		this.numberOfRounds = numberOfRounds;
		
		baInputStream.close();
		din.close();
	}

	@Override
	public int getType() {
		return this.type;
	}

	@Override
	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
		dout.writeInt(this.type);
		
		dout.writeInt(this.numberOfRounds);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}
	
	public int getNumberOfRounds() {
		return numberOfRounds;
	}

}
