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
package it.bancaditalia.oss.vtl.impl.engine.mapping;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MappingTest
{
	private final static Logger LOGGER = LoggerFactory.getLogger(MappingTest.class);
	
	@Test
	public void mappingTest() throws IOException
	{
		Pattern pattern = Pattern.compile("^.*<[^!]* (?:class|to)=\"(.*?)\".*$");
		List<String> notFound = new ArrayList<>();
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(MappingTest.class.getResourceAsStream("OpsFactory.xml"), UTF_8)))
		{
			boolean failed = false;
			String line;
			while ((line = reader.readLine()) != null)
			{
				Matcher matcher = pattern.matcher(line);
				if (matcher.matches())
					try
					{
						Class<?> c = Class.forName(matcher.group(1));
						LOGGER.info("Class {} found", c.getName());
					}
					catch (ClassNotFoundException e)
					{
						LOGGER.error(matcher.group(1));
						notFound.add(matcher.group(1));
						failed = true;
					}
			}
			
			if (failed)
				fail("One or more classes not found: " + notFound);
		};
	}
}
