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

import static com.querydsl.core.types.dsl.Expressions.path;
import static com.querydsl.core.types.dsl.Expressions.stringPath;
import static it.bancaditalia.oss.vtl.config.ConfigurationManager.getLocalPropertyValue;
import static it.bancaditalia.oss.vtl.config.ConfigurationManager.registerSupportedProperties;
import static it.bancaditalia.oss.vtl.config.VTLProperty.Options.IS_REQUIRED;
import static it.bancaditalia.oss.vtl.impl.environment.util.JDBCConfiguration.getVTLType;
import static java.util.EnumSet.noneOf;
import static java.util.stream.Collectors.toMap;

import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import com.querydsl.sql.Configuration;
import com.querydsl.sql.RelationalPathBase;
import com.querydsl.sql.ddl.CreateTableClause;
import com.querydsl.sql.dml.SQLInsertClause;

import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.config.VTLProperty.Options;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.exceptions.VTLUndefinedObjectException;
import it.bancaditalia.oss.vtl.impl.environment.dataset.JDBCDataSet;
import it.bancaditalia.oss.vtl.impl.environment.util.JDBCConfiguration;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class DBMSEnvironment implements Environment
{
	private static final Logger LOGGER = LoggerFactory.getLogger(DBMSEnvironment.class);
	
	public static final VTLProperty DRIVER_JAR = new VTLPropertyImpl("vtl.dbms.additional.classpath", "If set, the classpath will be appended before attempting to connect", "", noneOf(Options.class));
	public static final VTLProperty JDBC_URL = new VTLPropertyImpl("vtl.dbms.jdbc.url", "The URL of the DBMS possibly with properties", "", EnumSet.of(IS_REQUIRED));
	public static final VTLProperty JDBC_USER = new VTLPropertyImpl("vtl.dbms.jdbc.user", "The user name to access the DBMS (optional)", "", noneOf(Options.class));
	public static final VTLProperty JDBC_PASSWORD = new VTLPropertyImpl("vtl.dbms.jdbc.password", "The password to access the DBMS (optional)", "", noneOf(Options.class));
	public static final VTLProperty DATA_SCHEMA = new VTLPropertyImpl("vtl.dbms.data.schema", "The name of the schema containing data", "", EnumSet.of(IS_REQUIRED));
	public static final VTLProperty STORE_SCHEMA = new VTLPropertyImpl("vtl.dbms.store.schema", "The name of a schema that supports storing data", "", noneOf(Options.class));

	public static final String LINEAGE_COLNAME = "$$LINEAGE$$";

	static
	{
		registerSupportedProperties(DBMSEnvironment.class, DRIVER_JAR, JDBC_URL, JDBC_USER, JDBC_PASSWORD, DATA_SCHEMA, STORE_SCHEMA);
	}
	
	private final JDBCConfiguration jdbcConf;

	public DBMSEnvironment() throws MalformedURLException, SQLException
	{
		this(getLocalPropertyValue(JDBC_URL), getLocalPropertyValue(JDBC_USER), getLocalPropertyValue(JDBC_PASSWORD), 
			getLocalPropertyValue(DATA_SCHEMA), getLocalPropertyValue(STORE_SCHEMA), getLocalPropertyValue(DRIVER_JAR));
	}
	
	public DBMSEnvironment(String jdbcURL, String user, String password, String dataSchema, String storeSchema, String jarFile) throws SQLException
	{
		jdbcConf = new JDBCConfiguration(jdbcURL, user, password, dataSchema, storeSchema, jarFile); 
	}

	@Override
	public Optional<VTLValue> getValue(MetadataRepository repo, VTLAlias alias)
	{
		VTLValueMetadata meta = repo.getMetadata(alias).orElseThrow(() -> new VTLUndefinedObjectException("Dataset", alias));
		if (meta instanceof ScalarValueMetadata || alias.isComposed() || !alias.getName().matches("^[A-Za-z0-9_$?!%<>:]+$"))
			return Optional.empty();
		
		DataSetStructure structure = (DataSetStructure) meta;
		Connection conn = jdbcConf.connect();
		
        try (ResultSet rs = conn.getMetaData().getColumns(null, jdbcConf.quote(jdbcConf.getDataSchema()), jdbcConf.quote(alias.getName()), null))
        {
        	while (rs.next())
        	{
        	    VTLAlias columnAlias = VTLAliasImpl.of(true, rs.getString("COLUMN_NAME"));
				DataSetComponent<?, ?, ?> comp = structure.getComponent(columnAlias)
        	    	.orElseThrow(() -> new VTLMissingComponentsException(structure, columnAlias));
				
				JDBCType datatype = JDBCType.valueOf(rs.getInt("DATA_TYPE"));
				ValueDomainSubset<?, ?> domain = comp.getDomain();
				while (domain.getParentDomain() != domain && domain.getParentDomain() != null)
					domain = (ValueDomainSubset<?, ?>) domain.getParentDomain();
				
				if (!domain.isAssignableFrom(getVTLType(datatype).getDomain()))
					throw new IllegalStateException("Type " + datatype + " of column " + columnAlias + " is incompatible with " + comp);
        	}

            return Optional.of(new JDBCDataSet(alias, structure, jdbcConf));
        }
		catch (SQLException e)
		{
			throw new VTLNestedException("Error connecting to the dbms", e);
		}
	}
	
//	@Override
//	public boolean store(VTLValue value, VTLAlias alias)
//	{
//		if (!(value instanceof DataSet))
//			return false;
//		
//		DataSet ds = (DataSet) value;
//		if (ds instanceof JDBCDataSet)
//			return ((JDBCDataSet) ds).store(alias);
//		
//		return false;
//	}

	public void loadDatasets(Map<String, DataSet> datasets) throws SQLException
	{
		Connection conn = jdbcConf.connect();
		
		for (Entry<String, DataSet> entry: datasets.entrySet())
		{
			String table = jdbcConf.quote(entry.getKey());
			DataSet dataset = entry.getValue();
			Map<DataSetComponent<?, ?, ?>, Class<?>> types = dataset.getMetadata().stream().collect(toMap(c -> c, c -> c.getDomain().getRepresentation()));
			
			CreateTableClause create = new CreateTableClause(conn, new Configuration(jdbcConf.getTemplates()), table);
			for (DataSetComponent<?, ?, ?> comp: dataset.getMetadata())
			{
				Class<?> type = types.get(comp);
				create.column(jdbcConf.quote(comp.getAlias().getName()), type);
				if (type == String.class)
					create.size(32767);
			}
			create.column(LINEAGE_COLNAME, String.class).size(255);
			create.execute();

			RelationalPathBase<?> target = new RelationalPathBase<>(Object.class, "target", jdbcConf.getStoreSchema(), table);
			SQLInsertClause insert = new SQLInsertClause(conn, jdbcConf.getTemplates(), target);
			insert.setBatchToBulk(true);
			
			for (DataPoint dp: (Iterable<DataPoint>) dataset.stream()::iterator)
			{
				for (DataSetComponent<?, ?, ?> comp: dp.keySet())
					insert.set(path(Object.class, target, jdbcConf.quote(comp.getAlias().getName())), dp.get(comp).get());
				insert.set(stringPath(LINEAGE_COLNAME), dp.getLineage().toString());
				insert.addBatch();
			}
			
			LOGGER.info("Inserted {} rows in {}", insert.execute(), table);
		}
	}
}
