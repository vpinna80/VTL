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
package it.bancaditalia.oss.vtl.impl.environment.util;

import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.io.File.pathSeparator;
import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.Properties;
import java.util.ServiceLoader;

import com.querydsl.sql.Configuration;
import com.querydsl.sql.SQLTemplates;
import com.querydsl.sql.SQLTemplatesRegistry;

import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;

public class JDBCConfiguration implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final String jdbcURL;
	private final String dataSchema;
	private final String storeSchema;
	private final String jarFiles;
	private final String password;
	private final String user;
	private final ThreadLocal<Connection> pool = new ThreadLocal<>();
	
	private transient Driver driver;
	private transient SQLTemplates templates;
	private transient ClassLoader classLoader;
	private transient Configuration sqlConf;
	
	public JDBCConfiguration(String jdbcURL, String user, String password, String dataSchema, String storeSchema, String jarFiles)
	{
		this.jdbcURL = requireNonNull(jdbcURL);
		this.jarFiles = coalesce(jarFiles, "");
		this.user = user == null || user.isBlank() ? null : user.strip();
		this.password = password;
		
		// Test the jdbc connection
		try
		{
			this.dataSchema = quote(requireNonNull(dataSchema));
			this.storeSchema = quote(coalesce(storeSchema, dataSchema));
		}
		catch (SQLException e)
		{
			throw new VTLNestedException("Error connecting to dbms", e);
		}
	}

	private Driver getDriver() throws MalformedURLException
	{
		if (driver != null)
			return driver;
		
		try
		{
	        for (Driver driver: ServiceLoader.load(Driver.class, getClassLoader()))
	        	if (driver.acceptsURL(this.jdbcURL))
	        	{
	        		try (Connection conn = driver.connect(this.jdbcURL, new Properties()))
	        		{
	        			this.templates = new SQLTemplatesRegistry().getTemplates(conn.getMetaData());
	        		}
	        		
	        		return this.driver = driver;
	        	}
		}
		catch (SQLException e)
		{
			throw new VTLNestedException("Error connecting to the dbms", e);
		}

        throw new InvalidParameterException("No JDBC driver in classpath suitable for " + jdbcURL);
	}

	private ClassLoader getClassLoader() throws MalformedURLException
	{
		if (classLoader != null)
			return classLoader;
		
		ClassLoader classLoader;
		if (jarFiles != null && !jarFiles.isBlank() && Files.exists(Paths.get(jarFiles)))
		{
			String[] paths = jarFiles.split(pathSeparator);
			URL[] urls = new URL[paths.length];
			for (int i = 0; i < paths.length; i++)
				urls[i] = Paths.get(paths[i]).toUri().toURL();
			
			classLoader = new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
		}
		else
			classLoader = Thread.currentThread().getContextClassLoader();
		return this.classLoader = classLoader;
	}

	public String getDataSchema()
	{
		return dataSchema;
	}

	public String getStoreSchema()
	{
		return storeSchema;
	}
	
	public Connection connect()
	{
		try
		{
			Connection conn = pool.get();
			if (conn != null && !conn.isClosed())
				return conn;
			
			Properties info = new Properties();
			if (user != null)
				info.setProperty("user", user);
			if (password != null)
				info.setProperty("user", password);
			conn = getDriver().connect(jdbcURL, info);
			pool.set(conn);
			return conn;
		}
		catch (MalformedURLException | SQLException e)
		{
			throw new VTLNestedException("Error connecting to dbms", e);
		}
	}
	
	public String quote(String name) throws SQLException
	{
		return getTemplates().quoteIdentifier(name);
	}

	public Configuration getSQLConfiguration() throws SQLException
	{
		if (sqlConf != null)
			return sqlConf;
		
		return this.sqlConf = new Configuration(getTemplates());
	}

	public SQLTemplates getTemplates() throws SQLException
	{
		if (templates != null)
			return templates;
		
		return this.templates = new SQLTemplatesRegistry().getTemplates(connect().getMetaData());
	}

	public static Domains getVTLType(JDBCType jdbcType)
	{
		switch (jdbcType)
		{
			case BIGINT: return Domains.INTEGER;
			case BOOLEAN: return Domains.BOOLEAN;
			case BIT: return Domains.BOOLEAN;
			case CHAR: return Domains.STRING;
			case CLOB: return Domains.STRING;
			case DATE: return Domains.DATE;
			case DECIMAL: return Domains.NUMBER;
			case DOUBLE: return Domains.NUMBER;
			case FLOAT: return Domains.NUMBER;
			case INTEGER: return Domains.INTEGER;
			case LONGNVARCHAR: return Domains.STRING;
			case LONGVARCHAR: return Domains.STRING;
			case NCHAR: return Domains.STRING;
			case NCLOB: return Domains.STRING;
			case NUMERIC: return Domains.NUMBER;
			case NVARCHAR: return Domains.STRING;
			case REAL: return Domains.NUMBER;
			case SMALLINT: return Domains.INTEGER;
			case TIMESTAMP: return Domains.DATE;
			case TIMESTAMP_WITH_TIMEZONE: return Domains.DATE;
			case TINYINT: return Domains.INTEGER;
			case VARCHAR: return Domains.STRING;
			default: throw new UnsupportedOperationException("The JDBC type " + jdbcType + " is not supported.");
		}
	}
}
