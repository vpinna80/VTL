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
package it.bancaditalia.oss.vtl.impl.environment.dataset;

import static com.querydsl.core.types.Ops.EQ;
import static com.querydsl.core.types.dsl.Expressions.allOf;
import static com.querydsl.core.types.dsl.Expressions.as;
import static com.querydsl.core.types.dsl.Expressions.asBoolean;
import static com.querydsl.core.types.dsl.Expressions.booleanOperation;
import static com.querydsl.core.types.dsl.Expressions.comparablePath;
import static com.querydsl.core.types.dsl.Expressions.constant;
import static com.querydsl.core.types.dsl.Expressions.path;
import static com.querydsl.core.types.dsl.Expressions.stringPath;
import static com.querydsl.core.types.dsl.Expressions.stringTemplate;
import static com.querydsl.sql.SQLExpressions.countAll;
import static com.querydsl.sql.SQLExpressions.select;
import static com.querydsl.sql.SQLExpressions.unionAll;
import static it.bancaditalia.oss.vtl.impl.environment.DBMSEnvironment.LINEAGE_COLNAME;
import static it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl.createNumberValue;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.Option.DONT_SYNC;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.Collectors.toCollection;

import java.security.InvalidParameterException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.querydsl.core.Tuple;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.SubQueryExpression;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.BooleanOperation;
import com.querydsl.core.types.dsl.SimplePath;
import com.querydsl.sql.RelationalPathBase;
import com.querydsl.sql.SQLQuery;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.environment.util.JDBCConfiguration;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.AbstractDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageExternal;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;

public class JDBCDataSet extends AbstractDataSet
{
	private static final Logger LOGGER = LoggerFactory.getLogger(JDBCDataSet.class);
	
	private static final class JDBCIterator implements Iterator<DataPoint>
	{
		private final ResultSet rs;
		private final DataSetStructure structure;
		private final DataSetComponent<?, ?, ?>[] comps;
		private final int nCols;

		private boolean hasNext = false;
		
		private JDBCIterator(ResultSet rs, DataSetStructure structure) throws SQLException
		{
			this.rs = rs;
			this.structure = structure;
			this.nCols = rs.getMetaData().getColumnCount();
			
			comps = new DataSetComponent<?, ?, ?>[nCols - 1]; 
				
			// skip last column (it is the lineage)
			for (int i = 1; i < nCols; i++)
			{
				String columnName = coalesce(rs.getMetaData().getColumnLabel(i), rs.getMetaData().getColumnName(i));
				VTLAlias colAlias = VTLAliasImpl.of(true, columnName);
				comps[i - 1] = structure.getComponent(colAlias)
					.orElseThrow(() -> new VTLMissingComponentsException(structure, colAlias));
			}
		}
		
		@Override
		public synchronized boolean hasNext()
		{
			try
			{
				if (!hasNext)
					hasNext = !rs.isAfterLast() && rs.next();
				if (!hasNext)
					rs.close();
				
				return hasNext;
			}
			catch (SQLException e)
			{
				throw new VTLNestedException("Error retrieving data", e);
			}
		}
		
		@Override
		public synchronized DataPoint next()
		{
			try
			{
				if (!hasNext())
					throw new NoSuchElementException();
				
				hasNext = false;
				
				DataPointBuilder builder = new DataPointBuilder(DONT_SYNC);
				for (int i = 1; i < nCols; i++)
				{
					DataSetComponent<?, ?, ?> comp = comps[i - 1];
					builder = builder.add(comp, parseColumn(i, comp.getDomain()));
				}
				return builder.build(LineageExternal.of(rs.getString(nCols)), structure);
			}
			catch (SQLException e)
			{
				throw new VTLNestedException("Error retrieving data", e);
			}
		}
		
		private ScalarValue<?, ?, ?, ?> parseColumn(int colIndex, ValueDomainSubset<?, ?> domain) throws SQLException
		{
			Object value = rs.getObject(colIndex);
			if (value == null)
				return NullValue.instance(domain);
			
			ValueDomainSubset<?, ?> parentDomain = domain;
			while (!(parentDomain instanceof EntireDomainSubset))
				parentDomain = (ValueDomainSubset<?, ?>) parentDomain.getParentDomain();
			
			ScalarValue<?, ?, ?, ?> uncasted;
			if (parentDomain == INTEGERDS)
				uncasted = IntegerValue.of(rs.getLong(colIndex));
			else if (parentDomain == NUMBERDS)
				uncasted = createNumberValue((Number) value);
			else if (parentDomain == STRINGDS)
				uncasted = StringValue.of(rs.getString(colIndex));
			else if (parentDomain == DATEDS)
				uncasted = DateValue.of(rs.getObject(colIndex, LocalDate.class));
			else if (parentDomain == BOOLEANDS)
				uncasted = BooleanValue.of(rs.getBoolean(colIndex));
			else
				throw new UnsupportedOperationException("The domain " + domain + " is not supported with DBMSEnvironment");
			
			return domain.cast(uncasted);
		}
	}

	private final JDBCConfiguration jdbcConf;
	private final String table;
	private final RelationalPathBase<?> base;
	private final DataSetComponent<?, ?, ?>[] comps;

	public JDBCDataSet(VTLAlias alias, DataSetStructure structure, JDBCConfiguration jdbcConf)
	{
		super(structure);
		
		try
		{
			this.table = alias != null ? jdbcConf.quote(alias.getName()) : null;
		}
		catch (SQLException e)
		{
			throw new VTLNestedException("Error initializing JDBC table for " + alias, e);
		}
		
		this.jdbcConf = jdbcConf;
		this.base = alias != null ? new RelationalPathBase<>(Object.class, table, jdbcConf.getDataSchema(), table) : null;
		this.comps = structure.toArray(DataSetComponent<?, ?, ?>[]::new);
	}

	private JDBCDataSet(JDBCDataSet other, DataSetStructure newMetadata)
	{
		super(newMetadata);
		
		this.table = other.table;
		this.jdbcConf = other.jdbcConf;
		this.base = other.base;
		this.comps = newMetadata.toArray(DataSetComponent<?, ?, ?>[]::new);
	}

	@Override
	protected Stream<DataPoint> streamDataPoints()
	{
		try
		{
			SQLQuery<Tuple> query = generateSelect().clone(jdbcConf.connect());
			LOGGER.info("Performing query:\n*******************\n{}\n********************", query.getSQL().getSQL());
			ResultSet results = query.getResults();
			return StreamSupport.stream(spliteratorUnknownSize(new JDBCIterator(results, getMetadata()), IMMUTABLE + NONNULL), false);
		}
		catch (SQLException e)
		{
			throw new VTLNestedException("Error retrieving data for " + table, e);
		}
	}
	
	@Override
	public long size()
	{
		try (ResultSet rs = generateSelect().clone(jdbcConf.connect()).select(countAll).getResults())
		{
			rs.next();
			return rs.getLong(1);
		}
		catch (SQLException e)
		{
			throw new VTLNestedException("Error retrieving data for " + table, e);
		}
	}
	
	protected Expression<?>[] getColumns()
	{
		List<Expression<?>> names = Arrays.stream(comps)
			.map(DataSetComponent::getAlias)
			.map(VTLAlias::getName)
			.map(n -> as(path(Object.class, base, n), n))
			.collect(toCollection(ArrayList::new));
		names.add(stringPath(base, LINEAGE_COLNAME));
		
		return names.toArray(Expression[]::new);
	}
	
	protected SQLQuery<Tuple> generateSelect() 
	{
		return select(getColumns()).from(base); 
	}
	
	private Expression<?> getLineageExpr(SerUnaryOperator<Lineage> lineageOperator, Path<?> parent)
	{
		String placeholder = "{{LINEAGE}}";
		String template = lineageOperator.apply(LineageExternal.of(placeholder)).toString();
		return stringTemplate("replace('" + template + "', '" + placeholder + "', {0})", stringPath(parent, LINEAGE_COLNAME))
			.as(LINEAGE_COLNAME);
	}

	@Override
	public DataSet membership(VTLAlias alias, SerUnaryOperator<Lineage> lineageOperator)
	{
		DataSetStructure membershipStructure = dataStructure.membership(alias);
		LOGGER.debug("Creating dataset by membership on {} from {} to {}", alias, dataStructure, membershipStructure);
		
		DataSetComponent<?, ?, ?> sourceComponent = dataStructure.getComponent(alias)
				.orElseThrow(() -> new VTLMissingComponentsException(dataStructure, alias));
		DataSetComponent<? extends NonIdentifier, ?, ?> newMeasure = membershipStructure.getMeasures().iterator().next();

		List<Expression<?>> columns = membershipStructure.stream()
			.filter(c -> !c.equals(newMeasure))
			.map(DataSetComponent::getAlias)
			.map(VTLAlias::getName)
			.map(n -> path(Object.class, base, n).as(n))
			.collect(toCollection(ArrayList::new));
		columns.add(path(Object.class, base, sourceComponent.getAlias().getName()).as(newMeasure.getAlias().getName()));
		columns.add(getLineageExpr(lineageOperator, base));

		return new JDBCDataSet(this, membershipStructure) {
			@Override
			protected Expression<?>[] getColumns()
			{
				return columns.toArray(Expression<?>[]::new);
			}
		};
	}
	
	@Override
	public DataSet subspace(Map<? extends DataSetComponent<? extends Identifier, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> keyValues, SerUnaryOperator<Lineage> lineageOperator)
	{
		DataSetStructure subspaceStructure = dataStructure.subspace(keyValues.keySet());

		List<Expression<?>> columns = subspaceStructure.stream()
			.map(DataSetComponent::getAlias)
			.map(VTLAlias::getName)
			.map(n -> path(Object.class, base, n).as(n))
			.collect(toCollection(ArrayList::new));
		columns.add(getLineageExpr(lineageOperator, base));

		List<BooleanOperation> where = keyValues.entrySet().stream()
			.map(splitting((k, v) -> booleanOperation(EQ, path(Object.class, base, k.getAlias().getName()), constant(v.get()))))
			.collect(toList());
		
		return new JDBCDataSet(this, subspaceStructure) {
			@Override
			protected Expression<?>[] getColumns()
			{
				return columns.toArray(Expression[]::new);
			}
			
			@Override
			protected SQLQuery<Tuple> generateSelect()
			{
				return super.generateSelect().clone().where(allOf(where.toArray(BooleanExpression[]::new)));
			}
		};
	}
	
	@Override
	public DataSet union(List<DataSet> others, SerUnaryOperator<Lineage> lineageOp, boolean check)
	{
		if (others.stream().allMatch(JDBCDataSet.class::isInstance))
		{
			for (DataSet other: others)
				if (!dataStructure.equals(other.getMetadata()))
					throw new InvalidParameterException("Union between two datasets with different structures: " + dataStructure + " and " + other.getMetadata());

			List<DataSet> datasets = new ArrayList<>(others.size() + 1);
			List<SubQueryExpression<Tuple>> queries = new ArrayList<>();
			datasets.add(this);
			datasets.addAll(others);
			for (int i = 0; i < datasets.size(); i++)
			{
				JDBCDataSet dataset = (JDBCDataSet) datasets.get(i);
				SQLQuery<Tuple> query = dataset.generateSelect().clone();
				Expression<?>[] newCols = Arrays.copyOf(dataset.getColumns(), dataset.comps.length + 2);
				newCols[newCols.length - 1] = as(constant(i), "$$DS_NUMBER$$");
				queries.add(query.select(newCols));
			}

			List<Expression<?>> groupBy = new ArrayList<>();
			for (int i = 0; i < comps.length; i++)
				if (comps[i].is(Identifier.class))
					groupBy.add(path(Object.class, comps[i].getAlias().getName()));
			
			String unionAlias = "union_" + System.currentTimeMillis();
			Expression<?>[] groupByArray = groupBy.toArray(Expression<?>[]::new);
			Expression<?>[] groupByAndMin = Arrays.copyOf(groupByArray, groupByArray.length + 1);
			groupByAndMin[groupByAndMin.length - 1] = comparablePath(Comparable.class, "$$DS_NUMBER$$").min().as("$$DS_NUMBER$$");
			
			SimplePath<Object> leftPath = path(Object.class, unionAlias + "1");
			SimplePath<Object> rightPath = path(Object.class, unionAlias + "2");
			
			List<BooleanExpression> on = Arrays.stream(comps)
				.filter(c -> c.is(Identifier.class))
				.map(DataSetComponent::getAlias)
				.map(VTLAlias::getName)
				.map(n -> path(Object.class, leftPath, n).eq(path(Object.class, rightPath, n)))
				.collect(toCollection(ArrayList::new));
			on.add(path(Object.class, leftPath, "$$DS_NUMBER$$").eq(path(Object.class, rightPath, "$$DS_NUMBER$$")));
			
			List<Expression<?>> columns = Arrays.stream(comps)
				.map(DataSetComponent::getAlias)
				.map(VTLAlias::getName)
				.map(n -> path(Object.class, leftPath, n))
				.collect(toCollection(ArrayList::new));
			columns.add(getLineageExpr(lineageOp, leftPath));
			Expression<?>[] columnsArray = columns.toArray(Expression<?>[]::new);
 
			SQLQuery<Tuple> query = select(columnsArray)
				.from(unionAll(queries), leftPath)
				.innerJoin(select(groupByAndMin).from(unionAll(queries).as(unionAlias + "3")).groupBy(groupByArray), rightPath)
				.on(allOf(on.toArray(BooleanExpression[]::new)));

			return new JDBCDataSet(null, getMetadata(), jdbcConf) {
				
				@Override
				protected Expression<?>[] getColumns()
				{
					return columnsArray;
				}
				
				@Override
				protected SQLQuery<Tuple> generateSelect()
				{
					return query;
				}
			};
		}
		else
			return super.union(others, lineageOp, check);
	}

	@Override
	public DataSet filteredMappedJoin(DataSetStructure metadata, DataSet other, DataSetComponent<?, ?, ?> having, SerBinaryOperator<Lineage> lineageOp)
	{
		Set<DataSetComponent<Identifier, ?, ?>> ids = getMetadata().getIDs();
		Set<DataSetComponent<Identifier, ?, ?>> otherIds = other.getMetadata().getIDs();
		Set<DataSetComponent<Identifier, ?, ?>> commonIds = new HashSet<>(ids);
		commonIds.retainAll(otherIds);

		return new JDBCDataSet(null, metadata, jdbcConf) {
			@Override
			protected SQLQuery<Tuple> generateSelect()
			{
				SQLQuery<Tuple> join = super.generateSelect().clone();
				return join.having(path(Object.class, having.getAlias().getName()).eq(asBoolean(true)));
			}
		};
	}
	
//	public boolean store(VTLAlias alias)
//	{
//		Connection conn = jdbcConf.connect();
//		String store = jdbcConf.getStoreSchema();
//
//		try
//		{
//			String destTable = jdbcConf.quote(alias.getName());
//			
//			try (ResultSet rs = conn.getMetaData().getTables(null, store, destTable, new String[] { "TABLE" }))
//			{
//		        if (rs.next())
//		        	conn.createStatement().execute("TRUNCATE TABLE " + store + "." + destTable);
//		        else
//		        {
//					CreateTableClause create = new CreateTableClause(conn, new Configuration(jdbcConf.getTemplates()), destTable);
//					for (DataSetComponent<?, ?, ?> comp: getMetadata())
//						create.column(jdbcConf.quote(comp.getAlias().getName()), comp.getDomain().getRepresentation());
//					create.column(jdbcConf.quote(LINEAGE_COLNAME), String.class).size(200);
//					create.execute();
//		        }
//			}
//
//			return new SQLInsertClause(conn, jdbcConf.getTemplates(), new RelationalPathBase<>(Object.class, "target", store, table))
//				.select(generateSelect())
//				.execute() > 0;
//		}
//		catch (SQLException e)
//		{
//			throw new VTLNestedException("Error populating table " + table + " in store " + store, e);
//		}
//	}
}
