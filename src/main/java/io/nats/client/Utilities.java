/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

class Utilities {

	/**
	 * Convert a string containing only ASCII characters
	 * to the equivalent byte array. 
	 * 
	 * @param str the string to convert. 
	 * @return a byte array containing the character bytes of 
	 * of each character composing the input string.
	 */
	public static byte[] stringToBytesASCII(String str) {
		byte[] b = new byte[str.length()];
		for (int i = 0; i < b.length; i++) {
			b[i] = (byte) str.charAt(i);
		}
		return b;
	}

	
    /**
     * Copy an ASCII string to the specified offset in a byte array.
     *  
     * This method assumes that the input string value is ASCII
     * (single byte) encoding. Please do not arbitrarily change this 
     * method without doing your research. It is currently the fastest
     * way to perform this operation as of Java 1.6.
     * 
     * @param buffer the destination byte array
     * @param offset the buffer offset to write the string's bytes to.
     * @param str the string to copy 
     *
     */
	public static int stringToBytesASCII(byte[] buffer, int offset, String str) {
		int length = str.length();
		int end = offset + length;
		for (int i = 0; i < length; i++) {
			buffer[i+offset] = (byte)str.charAt(i);
		}
		return end;
	}
	
//	/**
//	 * Convert a string that is encoded in the system default 
//	 * (2-byte) character set to the equivalent byte array.
//	 * 
//	 * @param str the string to convert.
//	 * @return an array of the bytes that make up the input
//	 * string (2 bytes per character). 
//	 */
//	public static byte[] stringToBytesUTFCustom(String str) {
//		byte[] b = new byte[str.length() << 1];
//		for(int i = 0; i < str.length(); i++) {
//			char strChar = str.charAt(i);
//			int bpos = i << 1;
//			b[bpos] = (byte) ((strChar&0xFF00)>>8);
//			b[bpos + 1] = (byte) (strChar&0x00FF);
//		}
//		return b;
//	}
//
//	/**
//	 * Convert a byte array to a string using the system default 
//	 * (2-byte) character encoding.
//	 * 
//	 * @param bytes - the array of bytes to convert.
//	 * @return a string composed of the 2-byte characters contained
//	 * in {@code bytes}.
//	 */
//	public static String bytesToStringUTFCustom(byte[] bytes) {
//		char[] buffer = new char[bytes.length >> 1];
//		for(int i = 0; i < buffer.length; i++) {
//			int bpos = i << 1;
//			char c = (char)(((bytes[bpos]&0x00FF)<<8) + (bytes[bpos+1]&0x00FF));
//			buffer[i] = c;
//		}
//		return new String(buffer);
//	}

	final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();

	/**
	 * <b>bytesToHex</b>
	 * <p>
	 * Converts an array of bytes to its hexadecimal string representation.
	 * <p>
	 * {@code public static String bytesToHex(byte[] bytes) }
	 * <p>
	 * @param bytes - the byte array to convert
	 * @return		the hexidecimal string representation of {@code bytes}.
	 */
	public static String bytesToHex(byte[] bytes) {
	    char[] hexChars = new char[bytes.length * 2];
	    for ( int j = 0; j < bytes.length; j++ ) {
	        int v = bytes[j] & 0xFF;
	        hexChars[j * 2] = hexArray[v >>> 4];
	        hexChars[j * 2 + 1] = hexArray[v & 0x0F];
	    }
	    return new String(hexChars);
	}

}
