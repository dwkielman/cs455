package cs455.overlay.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * When a MessagingNode exits it should deregister itself it does so by sending a message to the registry
 * Message Type (int): DEREGISTER_REQUEST (6002)
 * Node IP address (String)
 * Node Port number (int)
 */

public class DeregisterRequest implements Event {

	private final int type = Protocol.DEREGISTER_REQUEST;
	private String IPAddress;
	private int portNumber;
	
	public DeregisterRequest(String IPAddress, int portNumber) {
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
	public DeregisterRequest(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.DEREGISTER_REQUEST) {
			System.out.println("Invalid Message Type for DeregisterRequest");
			return;
		}
		
		// IPAddress
		int IPAddressLength = din.readInt();
		byte[] IPAddressBytes = new byte[IPAddressLength];
		din.readFully(IPAddressBytes);
		
		this.IPAddress = new String(IPAddressBytes);
		
		// portNumber
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
		
		// IPAddress
		byte[] IPAddressBytes = this.IPAddress.getBytes();
		int IPAddressLength = IPAddressBytes.length;
		dout.writeInt(IPAddressLength);
		dout.write(IPAddressBytes);
		
		// portNumber
		dout.writeInt(this.portNumber);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}

	public String getIPAddress() {
		return IPAddress;
	}

	public int getPortNumber() {
		return portNumber;
	}

}
