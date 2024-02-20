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
package it.bancaditalia.oss.vtl.impl.environment;

import java.io.IOException;
import java.io.PrintStream;

import org.rosuda.JRI.RConsoleOutputStream;
import org.rosuda.JRI.Rengine;

public class RUtils
{
	public static final Rengine RENGINE = new Rengine();

	private static class WrappedOS extends RConsoleOutputStream {

		public WrappedOS(Rengine rengine, int mode)
		{
			super(rengine, mode);
		}

		@Override
		public void close() throws IOException
		{
			resetIO();
			super.close();
		}
	}

	static 
	{
		if (RENGINE == null)
			throw new ExceptionInInitializerError("Cannot initialize Rengine outside R");
		resetIO();
	}
	
	private RUtils() {}
	
	public static void resetIO()
	{
		System.setOut(new PrintStream(new WrappedOS(RENGINE, 0)));
		System.setErr(new PrintStream(new WrappedOS(RENGINE, 1)));
	}
}
