package cs455.overlay.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Used for when the Registry receives a request, it checks to see if the node had previously registered and ensures
 * the IP address in the message matches the address where the request originated
 * Message Type (int): REGISTER_RESPONSE (6001)
 * Status Code (byte): SUCCESS or FAILURE
 * Additional Info (String):
 */
public class RegisterResponse implements Event {

	private final int type = Protocol.REGISTER_RESPONSE;
	private byte statusCode;
	private String additionalInfo;
	
	public RegisterResponse(byte statusCode, String additionalInfo) {
		this.statusCode = statusCode;
		this.additionalInfo = additionalInfo;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * statusCode
	 * additionalInfo
	 * @throws IOException 
	 */
	public RegisterResponse(byte[] marshalledBytes) throws IOException {
		System.out.println("Begin RegisterResponse Sending of Event");
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.REGISTER_RESPONSE) {
			System.out.println("Invalid Message Type for RegisterResponse");
			return;
		}
		
		this.statusCode = din.readByte();
		
		int additionalInfoLength = din.readInt();
		byte[] additionalInfoBytes = new byte[additionalInfoLength];
		din.readFully(additionalInfoBytes);
		
		this.additionalInfo = new String(additionalInfoBytes);

		baInputStream.close();
		din.close();
		System.out.println("End RegisterResponse Sending of Event");
	}

	@Override
	public int getType() {
		return this.type;
	}

	@Override
	public byte[] getBytes() throws IOException {
		System.out.println("Begin RegisterResponse getBytes");
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
		dout.writeInt(this.type);
		
		dout.writeByte(this.statusCode);
		
		byte[] additionalInfoBytes = this.additionalInfo.getBytes();
		int additionalInfoLength = additionalInfoBytes.length;
		dout.writeInt(additionalInfoLength);
		dout.write(additionalInfoBytes);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		System.out.println("End RegisterResponse getBytes");
		return marshalledBytes;
	}

	public byte getStatusCode() {
		return statusCode;
	}

	public String getAdditionalInfo() {
		return additionalInfo;
	}

}
