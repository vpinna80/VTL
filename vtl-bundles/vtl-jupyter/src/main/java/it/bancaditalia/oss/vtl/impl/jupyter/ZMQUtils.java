/*
 * Copyright © 2020 Banca D'Italia
 *
 * Licensed under the EUPL, Version 1.2 (the "License");
 * You may not use this work except in compliance with the
 * License.
 * You may obtain a copy of the License at:
 *
 * https://joinup.ec.europa.eu/sites/default/files/custom-page/attachment/2020-03/EUPL-1.2%20EN.txt
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the License is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 *
 * See the License for the specific language governing
 * permissions and limitations under the License.
 */
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
