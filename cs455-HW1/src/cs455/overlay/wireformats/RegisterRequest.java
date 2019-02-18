package cs455.overlay.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Used for nodes requesting registering with the registry
 * Message Type (int): REGISTER_REQUEST (6000)
 * IP address (String)
 * Port number (int)
 */
public class RegisterRequest implements Event {

	private final int type = Protocol.REGISTER_REQUEST;
	private String IPAddress;
	private int portNumber;
	
	public RegisterRequest(String IPAddress, int portNumber) {
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
	public RegisterRequest(byte[] marshalledBytes) throws IOException {
		System.out.println("Begin RegisterRequest Sending of Event");
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.REGISTER_REQUEST) {
			System.out.println("Invalid Message Type for RegisterRequest");
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
		System.out.println("End RegisterRequest Sending of Event");
	}

	@Override
	public int getType() {
		return this.type;
	}

	@Override
	public byte[] getBytes() throws IOException {
		System.out.println("Begin RegisterRequest getBytes");
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
		System.out.println("End RegisterRequest getBytes");
		return marshalledBytes;
	}

	public String getIPAddress() {
		return IPAddress;
	}

	public int getPortNumber() {
		return portNumber;
	}

}
