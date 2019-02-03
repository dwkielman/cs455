package cs455.overlay.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Once a node has completed its task of sending a certain number of messages in rounds, it informs the registry of its task completion.
 * Message Type: TASK_COMPLETE (6007)
 * Node IP address (String):
 * Node Port number (int):
 */

public class TaskComplete implements Event {

	private final int type = Protocol.TASK_COMPLETE;
	private String IPAddress;
	private int portNumber;
	
	public TaskComplete(String IPAddress, int portNumber) {
		this.IPAddress = IPAddress;
		this.portNumber = portNumber;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * IPAddress
	 * portNumber
	 * @throws IOException 
	 */
	public TaskComplete(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.TASK_COMPLETE) {
			System.out.println("Invalid Message Type for TaskComplete");
			return;
		}
		
		int IPAddressLength = din.readInt();
		byte[] IPAddressBytes = new byte[IPAddressLength];
		din.readFully(IPAddressBytes);
		
		this.IPAddress = new String(IPAddressBytes);
		
		int portNumber = din.readInt();

		this.portNumber = portNumber;
		
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
		
		byte[] IPAddressBytes = this.IPAddress.getBytes();
		int IPAddressLength = IPAddressBytes.length;
		dout.writeInt(IPAddressLength);
		dout.write(IPAddressBytes);
		
		dout.writeInt(this.portNumber);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}

}
