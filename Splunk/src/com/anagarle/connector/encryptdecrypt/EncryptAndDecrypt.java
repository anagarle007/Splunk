package com.anagarle.connector.encryptdecrypt;

import java.io.UnsupportedEncodingException;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Scanner;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.annotation.adapters.HexBinaryAdapter;

public class EncryptAndDecrypt
{
  private static final String ENCRYPTION_KEY = "Securonix_Suites";
  private static final String ALGORITHM = "Rijndael";
  
  public EncryptAndDecrypt() {}
  
  public static String encrypt(String valueToEnc)
    throws Exception
  {
    Key key = generateKey(ENCRYPTION_KEY, ALGORITHM);
    Cipher c = Cipher.getInstance("AES");
    c.init(1, key);
    byte[] encValue = c.doFinal(valueToEnc.getBytes("UTF-8"));
    String encryptedValue = bytesToString(encValue);
    return encryptedValue;
  }
  
  public static String decrypt(String encryptedValue) throws Exception
  {
    if ((encryptedValue == null) || (encryptedValue.isEmpty()) || (encryptedValue.equalsIgnoreCase("null"))) {
      return "";
    }
    Key key = generateKey("Securonix_Suites", "AES");
    Cipher c = Cipher.getInstance("AES");
    c.init(2, key);
    byte[] decordedValue = hexToBytes(encryptedValue);
    byte[] decValue = c.doFinal(decordedValue);
    String decryptedValue = new String(decValue);
    return decryptedValue;
  }
  
  public static String decrypt(String encryptionKey, String encryptedValue) throws Exception
  {
    if ((encryptedValue == null) || (encryptedValue.isEmpty()) || (encryptedValue.equalsIgnoreCase("null"))) {
      return "";
    }
    Key key = generateKey(encryptionKey, "AES");
    Cipher c = Cipher.getInstance("AES");
    c.init(2, key);
    byte[] decordedValue = hexToBytes(encryptedValue);
    byte[] decValue = c.doFinal(decordedValue);
    String decryptedValue = new String(decValue);
    return decryptedValue;
  }
  
  public static String decrypt(List<String> sysKeyList, String encryptedValue) throws Exception
  {
    if ((encryptedValue == null) || (encryptedValue.isEmpty()) || (encryptedValue.equalsIgnoreCase("null")) || (sysKeyList == null) || (sysKeyList.isEmpty())) {
      return "";
    }
    String decryptedValue = null;
    for (String sysKey : sysKeyList)
    {
      Key key = generateKey(sysKey, "AES");
      Cipher c = Cipher.getInstance("AES");
      c.init(2, key);
      byte[] decordedValue = hexToBytes(encryptedValue);
      byte[] decValue = c.doFinal(decordedValue);
      decryptedValue = new String(decValue);
    }
    
    if ((decryptedValue == null) || (decryptedValue.trim().length() == 0))
    {
      throw new Exception("Could not decrypt data with existing keys");
    }
    return decryptedValue;
  }
  
  private static Key generateKey(String encryptionKey, String algorithm) throws Exception
  {
    Key key = new SecretKeySpec(encryptionKey.getBytes("UTF-8"), algorithm);
    return key;
  }
  
  public static String bytesToString(byte[] bytes) {
    HexBinaryAdapter adapter = new HexBinaryAdapter();
    String s = adapter.marshal(bytes);
    return s;
  }
  
  public static byte[] hexToBytes(String hexString) {
    HexBinaryAdapter adapter = new HexBinaryAdapter();
    byte[] bytes = adapter.unmarshal(hexString);
    return bytes;
  }
  
  public static String encrypt(String rawPassword, String algorithm) throws NoSuchAlgorithmException, UnsupportedEncodingException {
    MessageDigest digest = MessageDigest.getInstance(algorithm);
    String encryptedPassword = bytesToString(digest.digest(rawPassword.getBytes("UTF-8")));
    return encryptedPassword.toLowerCase();
  }
  
  public static void main(String[] args) throws Exception
  {
	 if(args.length==0)
	 {
		 Scanner reader = new Scanner(System.in); 
		 System.out.println("Enter Operation (encrypt or decrypt) : ");
	 
		 String operation = reader.next();
	 
		 if(operation.toLowerCase().equals("encrypt") || operation.toLowerCase().equals("decrypt"))
		 {
			 System.out.println("Enter value to " + operation + " :" );
			 String value = reader.next();
			 
			 if(operation.toLowerCase().equals("encrypt"))
				 System.out.print(encrypt(value) + "\n");
			 else
				 System.out.print(decrypt(value) + "\n");
		 }
		 else 
		 {
			 System.out.println("Invalid Operation.");
		    	System.exit(0);
		 }
		 reader.close();
	 }
  }
}
