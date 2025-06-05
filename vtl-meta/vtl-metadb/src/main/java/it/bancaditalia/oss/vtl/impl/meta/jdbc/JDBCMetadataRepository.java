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

import static it.bancaditalia.oss.vtl.config.ConfigurationManager.getLocalPropertyValue;
import static it.bancaditalia.oss.vtl.config.VTLProperty.Options.IS_REQUIRED;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.impl.meta.InMemoryMetadataRepository;
import it.bancaditalia.oss.vtl.impl.meta.subsets.IntegerCodeList;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.RangeIntegerDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.StringCodeList;
import it.bancaditalia.oss.vtl.impl.types.domain.StrlenDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.util.SerThrowingSupplier;

public class JDBCMetadataRepository extends InMemoryMetadataRepository
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(JDBCMetadataRepository.class);
	private static final Pattern EXTRACTOR = Pattern.compile("^.*?([+-]?\\d+).*?$");

	public static final VTLProperty METADATA_JDBC_URL = new VTLPropertyImpl("vtl.metadata.jdbc.url", "JDBC URL for the metadata db", "jdbc:", emptySet());
	public static final VTLProperty METADATA_JDBC_JNDI_POOL = new VTLPropertyImpl("vtl.metadata.jdbc.jndi", "jndi path to defined a JDBC DataSource", "jndi/myDB", emptySet());
	public static final VTLProperty METADATA_JDBC_SIZE_REGEX = new VTLPropertyImpl("vtl.metadata.jdbc.regex", "Regex to recognize length restriction; must contain three groups", "jndi/myDB", 
			EnumSet.of(IS_REQUIRED), "_(POS|POSNEG)_L(\\d+)_D(\\d+)$");

	static
	{
		ConfigurationManager.registerSupportedProperties(JDBCMetadataRepository.class, METADATA_JDBC_URL);
		ConfigurationManager.registerSupportedProperties(JDBCMetadataRepository.class, METADATA_JDBC_JNDI_POOL);
		ConfigurationManager.registerSupportedProperties(JDBCMetadataRepository.class, METADATA_JDBC_SIZE_REGEX);
	}

	private final SerThrowingSupplier<Connection, SQLException> pool;
	private final Pattern sizePattern;
	private final Map<VTLAlias, ValueDomainSubset<?, ?>> domains = new HashMap<>();
	
	public JDBCMetadataRepository() throws NamingException
	{
		sizePattern = Pattern.compile(getLocalPropertyValue(METADATA_JDBC_SIZE_REGEX));
		
		if (getLocalPropertyValue(METADATA_JDBC_JNDI_POOL) == null)
		{
			String dsName = getLocalPropertyValue(METADATA_JDBC_JNDI_POOL);
			DataSource dataSource = (DataSource) new InitialContext().lookup(dsName);
			pool = dataSource::getConnection;
		}
		else
		{
			String url = Objects.requireNonNull(getLocalPropertyValue(METADATA_JDBC_URL), "JDBC URL must not be null");
			pool = () -> DriverManager.getConnection(url);
		}
	}
	
	@Override
	public Optional<ValueDomainSubset<?, ?>> getDomain(VTLAlias alias)
	{
		return Optional.<ValueDomainSubset<?, ?>>ofNullable(domains.get(alias)).or(() -> super.getDomain(alias));
	}

	@Override
	public Optional<VTLValueMetadata> getMetadata(VTLAlias name)
	{
		LOGGER.debug("Reading structure for {}", name);
		try (Connection conn = pool.get();
			PreparedStatement stat1 = conn.prepareStatement("SELECT VARIABLEID, ROLE, DOMAINID, SETID FROM STRUCTUREITEM WHERE CUBEID = ?"))
		{
			stat1.setString(1, name.toString());
			ResultSet rs = stat1.executeQuery();
			
			DataStructureBuilder builder = null;
			while (rs.next())
			{
				if (builder == null)
					builder = new DataStructureBuilder();
				
				// get variable attribute
				VTLAlias varName = VTLAliasImpl.of(rs.getString(1));
				Class<? extends Component> role = parseRole(rs.getString(2));
				
				ValueDomainSubset<?, ?> domain;
				VTLAlias domainName = VTLAliasImpl.of(rs.getString(3));
				Optional<VTLAlias> setId = Optional.ofNullable(VTLAliasImpl.of(rs.getString(4)));
				domain = setId.flatMap(this::getDomain)
						.or(() -> getDomain(domainName))
						.orElseGet(() -> {
							ValueDomainSubset<?, ?> newDomain = parseDomain(domainName);
							domains.put(domainName, newDomain);
							
							if (setId.isPresent())
							{
								newDomain = parseSubset(conn, newDomain, setId.get());
								domains.put(setId.get(), newDomain);
							}
							
							LOGGER.trace("Domain {} already defined as {}", domainName, newDomain);
							return newDomain;
						});
				
				DataStructureComponent<? extends Component, ?, ?> comp = createTempVariable(varName, domain).as(role);
				LOGGER.trace("Read component {} for {}", comp, name);
				builder.addComponent(comp);
			}
			
			DataSetMetadata metadata = builder == null ? null : builder.build();
			LOGGER.debug("Structure for {} is {}", name, metadata);
			return Optional.of(metadata);
		}
		catch (SQLException | UncheckedIOException | NumberFormatException e)
		{
			LOGGER.error("Error while querying metadata for " + name, e);
			return Optional.empty();
		}
	}

	private Class<? extends Component> parseRole(String roleName)
	{
		Class<? extends Component> role; 
		switch (roleName.toLowerCase())
		{
			case "classification": role = Identifier.class; break;
			case "attribute": role = Attribute.class; break;
			case "measure": role = Measure.class; break;
			default: throw new UnsupportedOperationException("Unrecognized role: " + roleName);
		}
		return role;
	}

	private ValueDomainSubset<?, ?> parseDomain(VTLAlias domainName)
	{
		Matcher matcher = sizePattern.matcher(domainName.toString());
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
		if (domainName.getName().toLowerCase().contains("number"))
			if (sized)
			{
				long pow = 1;
				for (int i = l - d - 1; i >= 0; i--)
					pow *= 10;
				if (d == 0)
					domain = new RangeIntegerDomainSubset<>(domainName, INTEGERDS, OptionalLong.of(posOnly ? 0 : -pow + 1), OptionalLong.of(pow - 1), true);
				else
					throw new UnsupportedOperationException("Fixed-point decimals not supported: " + domainName);
			}
			else
				domain = NUMBERDS;
		else
			if (sized)
				domain = new StrlenDomainSubset(STRINGDS, OptionalInt.empty(), OptionalInt.of(l));
			else
				domain = STRINGDS;
		
		LOGGER.trace("domain {} is {}", domainName, domain);
		return domain;
	}
	
	private ValueDomainSubset<?, ?> parseSubset(Connection conn, ValueDomainSubset<?, ?> domain, VTLAlias setId)
	{
		try (PreparedStatement stat = conn.prepareStatement("SELECT CRITERIONPARAM FROM DOMAINSET WHERE SETID = ?"))
		{
			stat.setString(1, setId.toString());
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
				{
					Set<Long> set = stream
								.filter(JDBCMetadataRepository::matchesLong)
								.map(JDBCMetadataRepository::extractLong)
								.map(Long::valueOf)
								.peek(c -> LOGGER.trace("Subset {} has code {}", setId, c))
								.collect(toSet());
					
					return new IntegerCodeList(setId, (IntegerDomainSubset<?>) domain, set);
				}
				else
					return new StringCodeList((StringDomainSubset<?>) domain, setId, stream.peek(c -> LOGGER.trace("Found code {}" + c)).collect(toSet()));
			}
			else
			{
				LOGGER.trace("No codes found for set {} of domain {}", setId, domain);
				return domain;
			}
		}
		catch (NumberFormatException | SQLException e)
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
