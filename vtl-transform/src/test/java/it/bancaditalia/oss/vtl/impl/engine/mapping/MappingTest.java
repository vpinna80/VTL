package it.bancaditalia.oss.vtl.impl.engine.mapping;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
		Pattern pattern = Pattern.compile("^.*class=\"(.*?)\".*$");
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
						Class.forName(matcher.group(1));
					}
					catch (ClassNotFoundException e)
					{
						LOGGER.error(matcher.group(1));
						System.err.println(e.getMessage());
						failed = true;
					}
			}
			
			if (failed)
				fail("One or more classes not found");
		};
	}
}
