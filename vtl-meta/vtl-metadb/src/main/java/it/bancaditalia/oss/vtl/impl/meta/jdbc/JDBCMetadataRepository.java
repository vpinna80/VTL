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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static java.util.stream.Collectors.toSet;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.impl.meta.InMemoryMetadataRepository;
import it.bancaditalia.oss.vtl.impl.meta.subsets.IntegerCodeList;
import it.bancaditalia.oss.vtl.impl.meta.subsets.IntegerDomainRangeSubset;
import it.bancaditalia.oss.vtl.impl.meta.subsets.SizedStringDomainSubset;
import it.bancaditalia.oss.vtl.impl.meta.subsets.StringCodeList;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Attribute;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.util.SerSupplier;

public class JDBCMetadataRepository extends InMemoryMetadataRepository
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(JDBCMetadataRepository.class);
	private static final Pattern EXTRACTOR = Pattern.compile("^.*?([+-]?\\d+).*?$");

	public static final VTLProperty METADATA_JDBC_DRIVER_CLASS = new VTLPropertyImpl("vtl.metadata.jdbc.driver", "JDBC driver class name", "", false, false);
	public static final VTLProperty METADATA_JDBC_URL = new VTLPropertyImpl("vtl.metadata.jdbc.url", "JDBC URL for the metadata db", "jdbc:", false, false);
	public static final VTLProperty METADATA_JDBC_JNDI_POOL = new VTLPropertyImpl("vtl.metadata.jdbc.jndi", "jndi path to defined a JDBC DataSource", "jndi/myDB", false, false);
	public static final VTLProperty METADATA_JDBC_SIZE_REGEX = new VTLPropertyImpl("vtl.metadata.jdbc.regex", "Regex to recognize length restriction; must contain three groups", "jndi/myDB", 
			true, false, "_(POS|POSNEG)_L(\\d+)_D(\\d+)$");

	private final SerSupplier<Connection> pool;
	private final Pattern sizePattern;
	
	static
	{
		ConfigurationManagerFactory.registerSupportedProperties(JDBCMetadataRepository.class, METADATA_JDBC_DRIVER_CLASS);
		ConfigurationManagerFactory.registerSupportedProperties(JDBCMetadataRepository.class, METADATA_JDBC_URL);
		ConfigurationManagerFactory.registerSupportedProperties(JDBCMetadataRepository.class, METADATA_JDBC_JNDI_POOL);
		ConfigurationManagerFactory.registerSupportedProperties(JDBCMetadataRepository.class, METADATA_JDBC_SIZE_REGEX);
	}

	public JDBCMetadataRepository() throws InstantiationException, IllegalAccessException, ClassNotFoundException
	{
		sizePattern = Pattern.compile(METADATA_JDBC_SIZE_REGEX.getValue());
		
		if (METADATA_JDBC_JNDI_POOL.hasValue())
			pool = () -> {
				String ds = METADATA_JDBC_JNDI_POOL.getValue();
				try
				{
					return ((DataSource) new InitialContext().lookup(ds)).getConnection();
				}
				catch (SQLException | NamingException e)
				{
					throw new IllegalStateException("Error estabilishing connection via jndi path " + ds, e);
				}
			};
		else
			pool = () -> {
				String drv = Objects.requireNonNull(METADATA_JDBC_DRIVER_CLASS.getValue(), "JDBC driver class name must not be null");
				String url = Objects.requireNonNull(METADATA_JDBC_URL.getValue(), "JDBC URL must not be null");
				try
				{
					return Class.forName(drv).asSubclass(Driver.class).newInstance().connect(url, new Properties());
				}
				catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e)
				{
					throw new IllegalStateException("Error estabilishing connection to " + url, e);
				}
			};
	}

	@Override
	public DataSetMetadata getStructure(String name)
	{
		LOGGER.debug("Reading structure for {}", name);
		try (Connection conn = pool.get();
			PreparedStatement stat1 = conn.prepareStatement("SELECT VARIABLEID, ROLE, DOMAINID, SETID FROM STRUCTUREITEM WHERE CUBEID = ?"))
		{
			stat1.setString(1, name);
			ResultSet rs = stat1.executeQuery();
			
			DataStructureBuilder builder = null;
			while (rs.next())
			{
				if (builder == null)
					builder = new DataStructureBuilder();
				
				// get variable attribute
				String varName = rs.getString(1);
				Class<? extends ComponentRole> role = parseRole(rs.getString(2));
				
				ValueDomainSubset<?, ?> domain;
				String domainName = rs.getString(3);
				String setId = rs.getString(4);
				if (setId != null && !setId.isEmpty() && isDomainDefined(setId))
				{
					domain = getDomain(setId);
					LOGGER.trace("Set {} already defined as {}", setId, domain);
				}
				else
				{
					if (isDomainDefined(domainName))
						domain = getDomain(domainName);
					else
					{
						domain = defineDomain(domainName, parseDomain(domainName));
						LOGGER.trace("Domain {} already defined as {}", domainName, domain);
					}
					
					if (setId != null && !setId.isEmpty())
						domain = defineDomain(setId, parseSubset(conn, domain, setId));
				}
				
				DataStructureComponent<? extends ComponentRole, ?, ?> comp = DataStructureComponentImpl.of(varName, role, domain);
				LOGGER.trace("Read component {} for {}", comp, name);
				builder.addComponent(comp);
			}
			
			DataSetMetadata metadata = builder == null ? null : builder.build();
			LOGGER.debug("Structure for {} is {}", name, metadata);
			return metadata;
		}
		catch (SQLException | IOException | NumberFormatException e)
		{
			LOGGER.error("Error while querying metadata for " + name, e);
			return null;
		}
	}

	private Class<? extends ComponentRole> parseRole(String roleName)
	{
		Class<? extends ComponentRole> role; 
		switch (roleName.toLowerCase())
		{
			case "classification": role = Identifier.class; break;
			case "attribute": role = Attribute.class; break;
			case "measure": role = Measure.class; break;
			default: throw new UnsupportedOperationException("Unrecognized role: " + roleName);
		}
		return role;
	}

	private ValueDomainSubset<?, ?> parseDomain(String domainName)
	{
		Matcher matcher = sizePattern.matcher(domainName);
		boolean sized = matcher.find();
		int l;
		int d;
		boolean posOnly;
		if (sized)
		{
			l = Integer.parseInt(matcher.group(2));
			d = Integer.parseInt(matcher.group(3));
			posOnly = "POS".equalsIgnoreCase(matcher.group(1));
		}
		else
		{
			l = 0;
			d = 0;
			posOnly = false;
		}
		
		ValueDomainSubset<?, ?> domain;
		if (domainName.toLowerCase().contains("number"))
			if (sized)
			{
				long pow = 1;
				for (int i = l - d - 1; i >= 0; i--)
					pow *= 10;
				if (d == 0)
					domain = new IntegerDomainRangeSubset(domainName, posOnly ? 0 : -pow + 1, pow - 1, INTEGERDS);
				else
					throw new UnsupportedOperationException("Fixed-point decimals not supported: " + domainName);
			}
			else
				domain = NUMBERDS;
		else
			if (sized)
				domain = new SizedStringDomainSubset<>(domainName, STRINGDS, l);
			else
				domain = STRINGDS;
		
		LOGGER.trace("domain {} is {}", domainName, domain);
		return domain;
	}
	
	private ValueDomainSubset<?, ?> parseSubset(Connection conn, ValueDomainSubset<?, ?> domain, String setId) throws SQLException, IOException
	{
		try (PreparedStatement stat = conn.prepareStatement("SELECT CRITERIONPARAM FROM DOMAINSET WHERE SETID = ?"))
		{
			stat.setString(1, setId);
			ResultSet rs = stat.executeQuery();
			if (rs.next())
			{
				String criterionParam = rs.getString(1);
				LOGGER.trace("Set {} has criterion: {}", setId, criterionParam);
				Stream<String> stream = Arrays.stream(criterionParam.split(";"))
					.map(String::trim)
					.map(s -> s.split("=", 2))
					.filter(a -> a.length == 2)
					.map(a -> a[0].trim())
					.filter(s -> !s.isEmpty());
				if (INTEGERDS.isAssignableFrom(domain))
					return new IntegerCodeList<>((IntegerDomainSubset<?>) domain, setId, stream.filter(JDBCMetadataRepository::matchesLong).map(JDBCMetadataRepository::extractLong)
							.map(Long::valueOf).peek(c -> LOGGER.trace("Subset {} has code {}", setId, c)).collect(toSet()));
				else
					return new StringCodeList<>((StringDomainSubset<?>) domain, setId, stream.peek(c -> LOGGER.trace("Found code {}" + c)).collect(toSet()));
			}
			else
			{
				LOGGER.trace("No codes found for set {} of domain {}", setId, domain);
				return domain;
			}
		}
		catch (NumberFormatException e)
		{
			LOGGER.error("Error while reading codelist for " + setId, e);
			return domain;
		}
	}

	private static boolean matchesLong(String repr)
	{
		return EXTRACTOR.matcher(repr).matches();
	}

	private static Long extractLong(String repr)
	{
		Matcher m = EXTRACTOR.matcher(repr);
		if (m.find())
			return Long.valueOf(m.group(1));
		else
			throw new NumberFormatException("Cannot parse long: " + repr);
	}
}
