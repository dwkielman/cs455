package cs455.overlay.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Data can be fed into the network from any messaging node within the network. Packets are sent from a source to a sink.
 * A message includes a payload which is a random integer.
 * Message Type (int): MESSAGE (6010)
 * Source IP address (String)
 * Source Port number (int)
 * Destination IP address (String)
 * Destination Port number (int)
 * Payload (int)
 */

public class Message implements Event {

	private final int type = Protocol.MESSAGE;
	private String sourceIPAddress;
	private int sourcePortNumber;
	private String destiantionIPAddress;
	private int destiantionPortNumber;
	private int payload;
	
	public Message(String sourceIPAddress, int sourcePortNumber, String destiantionIPAddress, int destiantionPortNumber, int payload) {
		this.sourceIPAddress = sourceIPAddress;
		this.sourcePortNumber = sourcePortNumber;
		this.destiantionIPAddress = destiantionIPAddress;
		this.destiantionPortNumber = destiantionPortNumber;
		this.payload = payload;
	}

	/**
	 * byte[] construction is as follows:
	 * type
	 * sourceIPAddress
	 * sourcePortNumber
	 * destiantionIPAddress
	 * destiantionPortNumber
	 * payload
	 * @throws IOException 
	 */
	public Message(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.REGISTER_REQUEST) {
			System.out.println("Invalid Message Type for RegisterRequest");
			return;
		}
		
		int sourceIPAddressLength = din.readInt();
		byte[] sourceIPAddressBytes = new byte[sourceIPAddressLength];
		din.readFully(sourceIPAddressBytes);
		
		this.sourceIPAddress = new String(sourceIPAddressBytes);
		
		int sourcePortNumber = din.readInt();

		this.sourcePortNumber = sourcePortNumber;
		
		int destiantionIPAddressLength = din.readInt();
		byte[] destiantionIPAddressBytes = new byte[destiantionIPAddressLength];
		din.readFully(destiantionIPAddressBytes);
		
		this.destiantionIPAddress = new String(destiantionIPAddressBytes);
		
		int destiantionPortNumber = din.readInt();

		this.destiantionPortNumber = destiantionPortNumber;
		
		int payload = din.readInt();

		this.payload = payload;
		
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
		
		byte[] sourceIPAddressBytes = this.sourceIPAddress.getBytes();
		int sourceIPAddressLength = sourceIPAddressBytes.length;
		dout.writeInt(sourceIPAddressLength);
		dout.write(sourceIPAddressBytes);
		
		dout.writeInt(this.sourcePortNumber);
		
		byte[] destiantionIPAddressBytes = this.destiantionIPAddress.getBytes();
		int destiantionIPAddressLength = destiantionIPAddressBytes.length;
		dout.writeInt(destiantionIPAddressLength);
		dout.write(destiantionIPAddressBytes);
		
		dout.writeInt(this.destiantionPortNumber);
		
		dout.writeInt(this.payload);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}


}
