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