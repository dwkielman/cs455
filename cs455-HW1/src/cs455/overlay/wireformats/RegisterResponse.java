package cs455.overlay.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Used for when the Registry receives a request from a MessagingNode to register with it.
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
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.REGISTER_RESPONSE) {
			System.out.println("Invalid Message Type for RegisterResponse");
			return;
		}
		
		// statusCode
		this.statusCode = din.readByte();
		
		int additionalInfoLength = din.readInt();
		byte[] additionalInfoBytes = new byte[additionalInfoLength];
		din.readFully(additionalInfoBytes);
		
		// additionalInfo
		this.additionalInfo = new String(additionalInfoBytes);

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
		
		// statusCode
		dout.writeByte(this.statusCode);
		
		// additionalInfo
		byte[] additionalInfoBytes = this.additionalInfo.getBytes();
		int additionalInfoLength = additionalInfoBytes.length;
		dout.writeInt(additionalInfoLength);
		dout.write(additionalInfoBytes);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		return marshalledBytes;
	}

	public byte getStatusCode() {
		return statusCode;
	}

	public String getAdditionalInfo() {
		return additionalInfo;
	}

}
