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
package it.bancaditalia.oss.vtl.impl.meta.jdbc;

import static it.bancaditalia.oss.vtl.impl.meta.jdbc.JDBCMetadataRepository.METADATA_JDBC_URL;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.hsqldb.jdbc.JDBCDriver;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class JDBCMetadataRepositoryTest
{
	private static final String TEST_DB = "jdbc:hsqldb:mem:testdb";
	
	static
	{
	}
	
	@BeforeAll
	public static void init() throws IOException, SQLException
	{
		final Properties props = new Properties();
		props.setProperty("sql.syntax_ora", "true");
		try (Connection driver = JDBCDriver.driverInstance.connect(TEST_DB, props);
			BufferedReader br = new BufferedReader(new InputStreamReader(JDBCMetadataRepositoryTest.class.getResourceAsStream("init.sql"), UTF_8)))
		{
			for (String line = "", text = ""; line != null; text += (line = br.readLine()) + "\n")
				if (line.matches(".*[;]\\s*$"))
				{
					driver.createStatement().executeUpdate(text);
					text = "";
				}
		}

		METADATA_JDBC_URL.setValue(TEST_DB);
	}
	
	@Test
	public void test()
	{
		assertTrue(true);
	}
}