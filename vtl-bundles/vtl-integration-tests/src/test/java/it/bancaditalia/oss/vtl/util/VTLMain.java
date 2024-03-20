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

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.session.VTLSession;

public class VTLMain
{
	public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, URISyntaxException
	{
		try (BufferedReader reader = Files.newBufferedReader(Path.of(System.getProperty("user.home"), ".vtlStudio.properties")))
		{
			ConfigurationManagerFactory.loadConfiguration(reader);
		}
		
		try (BufferedReader reader = Files.newBufferedReader(Path.of(VTLMain.class.getResource("script.vtl").toURI())))
		{
			VTLSession session = ConfigurationManager.getDefault().createSession(reader);
			
			session.resolve("test", DataSet.class).forEach(System.out::println);
		}
	}
}