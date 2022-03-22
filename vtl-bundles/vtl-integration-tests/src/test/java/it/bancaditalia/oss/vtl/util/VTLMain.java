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
package it.bancaditalia.oss.vtl.util;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

import it.bancaditalia.oss.vtl.impl.session.VTLSessionImpl;
import it.bancaditalia.oss.vtl.model.data.DataSet;

public class VTLMain
{
	public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException
	{
		Reader reader = new InputStreamReader(VTLMain.class.getResourceAsStream("script.vtl"), StandardCharsets.UTF_8);
		VTLSessionImpl session = new VTLSessionImpl();
		session.addStatements(reader);
		session.compile();
		DataSet ds = session.resolve("ds_u", DataSet.class);
		Triple<String[], String[], Long[]> t = new LineageViewer(ds).generateAdiacenceMatrix(session);
		for (int i = 0; i < t.getFirst().length; i++)
			System.out.println(t.getFirst()[i] + " --- " + t.getSecond()[i] + " --- " + t.getThird()[i]);
	}
}