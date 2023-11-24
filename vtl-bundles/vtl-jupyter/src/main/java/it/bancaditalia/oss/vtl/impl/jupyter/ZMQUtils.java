package it.bancaditalia.oss.vtl.impl.jupyter;

import static java.lang.Character.digit;
import static java.lang.Character.forDigit;

public class ZMQUtils
{
	public static byte[] hexBytes(String hex)
	{
		final int length = hex.length() / 2;
		byte[] data = new byte[length];
		
		for (int i = 0; i < length; i++)
			data[i] = (byte) ((digit(hex.charAt(i + i), 16) << 4) | digit(hex.charAt(i + i + 1), 16));

		return data;
	}

	public static String hexString(byte bytes[])
	{
		StringBuilder builder = new StringBuilder(bytes.length * 2);
		
		for (byte b: bytes)
			builder.append(forDigit((b >>> 4) & 0xF, 16)).append(forDigit(b & 0xF, 16));

		return builder.toString();
	}
}
