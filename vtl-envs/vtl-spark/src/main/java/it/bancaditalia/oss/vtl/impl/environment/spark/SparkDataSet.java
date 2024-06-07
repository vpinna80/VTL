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
package it.bancaditalia.oss.vtl.impl.environment.spark;

import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkEnvironment.LineageSparkUDT;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkEnvironment.VTL_SPARK_PAGE_SIZE;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.createStructFromComponents;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.getColumnsFromComponents;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.getDataTypeFor;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.getEncoderFor;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.getMetadataFor;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.getNamesFromComponents;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.getScalarFor;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.toDataPoint;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NULLDS;
import static it.bancaditalia.oss.vtl.model.transform.analytic.LimitCriterion.LimitDirection.PRECEDING;
import static it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion.SortingMethod.ASC;
import static it.bancaditalia.oss.vtl.model.transform.analytic.WindowCriterion.LimitType.RANGE;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.mapping;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toArray;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithValues;
import static it.bancaditalia.oss.vtl.util.SerUnaryOperator.identity;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyMap;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static org.apache.spark.sql.Encoders.INT;
import static org.apache.spark.sql.expressions.Window.partitionBy;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.udaf;
import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.types.DataTypes.createArrayType;
import static scala.collection.JavaConverters.asScala;

import java.io.Serializable;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.execution.ExplainMode;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.Window$;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLInvariantIdentifiersException;
import it.bancaditalia.oss.vtl.impl.environment.spark.DataPointEncoder.DataPointImpl;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.AbstractDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageExternal;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.analytic.LimitCriterion;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowClause;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerBiPredicate;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerFunction;
import it.bancaditalia.oss.vtl.util.SerPredicate;
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;
import it.bancaditalia.oss.vtl.util.Utils;

public class SparkDataSet extends AbstractDataSet
{
	private static final Logger LOGGER = LoggerFactory.getLogger(SparkDataSet.class);
	private static final long serialVersionUID = 1L;

	private final SparkSession session;
	private final DataPointEncoder encoder;
	private final Dataset<Row> dataFrame;
	
	public SparkDataSet(SparkSession session, DataSetMetadata structure, DataPointEncoder encoder, Dataset<Row> dataFrame)
	{
		super(structure);
		
		for (DataStructureComponent<?, ?, ?> component: encoder.components)
		{
			String name = component.getVariable().getName();
			dataFrame = dataFrame.withColumn(name, dataFrame.col(name), getMetadataFor(component));
		}
		dataFrame = dataFrame.withColumn("$lineage$", dataFrame.col("$lineage$"), Metadata.empty());

		this.session = session;
		this.encoder = encoder;
		this.dataFrame = dataFrame;

		logInfo(toString());
	}

	public SparkDataSet(SparkSession session, DataSetMetadata structure, Dataset<Row> dataFrame)
	{
		this(session, structure, new DataPointEncoder(structure), dataFrame);
	}

	public SparkDataSet(SparkSession session, DataSetMetadata dataStructure, DataSet toWrap)
	{
		this(session, dataStructure, session.createDataset(collectToDF(toWrap), 
				new DataPointEncoder(toWrap.getMetadata()).getRowEncoder()).cache());
	}
	
	@Override
	protected Stream<DataPoint> streamDataPoints()
	{
		int bufferSize = Integer.parseInt(VTL_SPARK_PAGE_SIZE.getValue());
		LOGGER.warn("Streaming datapoints into the driver from Spark dataset {}", getMetadata());

		return StreamSupport.stream(new SparkSpliterator(dataFrame.toLocalIterator(), bufferSize), !Utils.SEQUENTIAL).map(encoder::decode);
	}
	
	@Override
	public boolean isCacheable()
	{
		return false;
	}

	public Dataset<Row> getDataFrame()
	{
		return dataFrame;
	}

	@Override
	public DataSet membership(String alias)
	{
		DataSetMetadata membershipStructure = getMetadata().membership(alias);
		LOGGER.debug("Creating dataset by membership on {} from {} to {}", alias, getMetadata(), membershipStructure);
		
		Dataset<Row> newDF = dataFrame;
		Optional<DataStructureComponent<?, ?, ?>> idMembership = getMetadata().getComponent(alias).filter(d -> d.is(Identifier.class));
		if (idMembership.isPresent())
		{
			String defaultName = idMembership.get().getVariable().getDomain().getDefaultVariable().getName();
			newDF = dataFrame.withColumn(defaultName, dataFrame.col(alias));
		}
		
		Column[] columns = getColumnsFromComponents(membershipStructure).toArray(new Column[membershipStructure.size()]);

		// order columns alphabetically
		newDF = newDF.select(columns);
		
		LOGGER.warn("Membership lineage not implemented");
		byte[] serializedLineage = LineageSparkUDT.serialize(LineageExternal.of("#" + alias));
		return new SparkDataSet(session, membershipStructure, newDF.withColumn("$lineage$", lit(serializedLineage)));
	}

	@Override
	public DataSet filter(SerPredicate<DataPoint> predicate, SerUnaryOperator<Lineage> lineageOperator)
	{
		FilterFunction<Row> func = row -> predicate.test(encoder.decode(row));
		DataSetMetadata structure = getMetadata();
		MapFunction<Row, Row> lineage = row -> {
			DataPoint dp = encoder.decode(row);
			DataPoint newDp = new DataPointBuilder(dp).build(lineageOperator.apply(dp.getLineage()), structure);
			return encoder.encode(newDp);
		};
		return new SparkDataSet(session, getMetadata(), dataFrame.filter(func).map(lineage, encoder.getRowEncoder()));
	}
	
	@Override
	public DataSet getMatching(Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> keyValues)
	{
		return filter(dp -> keyValues.equals(dp.getValues(keyValues.keySet(), Identifier.class)), SerUnaryOperator.identity());	
	}
	
	@Override
	public long size()
	{
		return dataFrame.count();
	}

	@Override
	public DataSet subspace(Map<? extends DataStructureComponent<? extends Identifier, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> keyValues, SerFunction<? super DataPoint, ? extends Lineage> lineageOperator)
	{
		DataSetMetadata newMetadata = new DataStructureBuilder(getMetadata()).removeComponents(keyValues.keySet()).build();
		DataPointEncoder newEncoder = new DataPointEncoder(newMetadata);
		
		Dataset<Row> newDf = dataFrame;
		for (Entry<? extends DataStructureComponent<? extends Identifier, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> e: keyValues.entrySet())
		{
			Column col = newDf.col(e.getKey().getVariable().getName());
			newDf = newDf.filter(col.equalTo(lit(e.getValue().get()))).drop(col);
		}
		
		newDf = newDf.withColumn("$lineage$", udf(dpLin -> LineageNode.of("sub " + keyValues, (Lineage) dpLin), LineageSparkUDT)
				.apply(newDf.col("$lineage$")));
		
		return new SparkDataSet(session, newMetadata, newEncoder, newDf);
	}
	
	@Override
	public DataSet mapKeepingKeys(DataSetMetadata metadata, SerFunction<? super DataPoint, ? extends Lineage> lineageOperator,
			SerFunction<? super DataPoint, ? extends Map<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>>> operator)
	{
		final Set<DataStructureComponent<Identifier, ?, ?>> originalIDs = getMetadata().getIDs();
		if (!metadata.getIDs().containsAll(originalIDs))
			throw new VTLInvariantIdentifiersException("map", originalIDs, metadata.getIDs());
		
		LOGGER.trace("Creating dataset by mapping from {} to {}", getMetadata(), metadata);
		
		// wrap the result of the mapping of the decoded datapoint into a struct
		UDF1<Row, Row> wrappedOperator = row -> {
			DataPoint dp = encoder.decode(row);
			Map<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> map = new HashMap<>(operator.apply(dp));
			map.keySet().removeAll(originalIDs);
			
			// compute values
			Object[] resultArray = map.entrySet().stream()
					.sorted((a, b) -> DataStructureComponent.byNameAndRole(a.getKey(), b.getKey()))
					.map(Entry::getValue)
					.map(ScalarValue::get)
					.collect(toArray(new Object[map.size() + 1]));
			
			// Compute lineage
			resultArray[resultArray.length - 1] = lineageOperator.apply(dp);
			
			return new GenericRow(resultArray);
		};

		// the struct for the result (from the result metadata) omitting the original ids
		Set<DataStructureComponent<?, ?, ?>> newComponents = new HashSet<>(metadata);
		newComponents.removeAll(originalIDs);
		List<StructField> fields = new ArrayList<>(createStructFromComponents(newComponents));
		fields.add(new StructField("$lineage$", LineageSparkUDT, false, Metadata.empty()));
		StructType structType = new StructType(fields.toArray(new StructField[fields.size()]));
		
		// wrap the source row into a struct for passing to the UDF
		Column wrappedRow = Arrays.stream(dataFrame.columns())
			.map(dataFrame::col)
			.collect(collectingAndThen(toList(), list -> struct(asScala((Iterable<Column>) list).toSeq())));
		
		// the columns to keep
		String[] ids = originalIDs.stream()
				.map(DataStructureComponent::getVariable).map(Variable::getName)
				.collect(toArray(new String[originalIDs.size()]));

		// apply the transformation to the data frame and unwrap the result
		Dataset<Row> dataFrame2 = dataFrame.withColumn("__extended", udf(wrappedOperator, structType).apply(wrappedRow))
				.select("__extended.*", ids);
		
		// reorder columns to match the encoder order
		Column[] ordered = getColumnsFromComponents(metadata).toArray(new Column[metadata.size() + 1]);
		ordered[ordered.length - 1] = dataFrame2.col("$lineage$");

		return new SparkDataSet(session, metadata, dataFrame2.select(ordered));
	}
	
	@Override
	public DataSet flatmapKeepingKeys(DataSetMetadata metadata, SerFunction<? super DataPoint, ? extends Lineage> lineageOperator,
			SerFunction<? super DataPoint, ? extends Stream<? extends Map<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>>>> operator)
	{
		DataPointEncoder resultEncoder = new DataPointEncoder(metadata);
		DataStructureComponent<?, ?, ?>[] comps = resultEncoder.components;
		Set<DataStructureComponent<Identifier, ?, ?>> ids = getMetadata().getIDs();
		StructType schema = resultEncoder.schema;
		
		Dataset<Row> flattenedDf = dataFrame.flatMap((FlatMapFunction<Row, Row>) row -> {
			DataPointImpl dp = encoder.decode(row);
			Serializable[] vals = new Serializable[comps.length + 1];
			vals[comps.length] = dp.getLineage();
			
			// Template of the result rows with the original id values
			for (int i = 0, j = 0; j < comps.length; j++)
				if (dp.getComp(i).equals(comps[j]) && ids.contains(comps[j]))
					vals[j] = dp.getValue(i++).get();
			
			return operator.apply(dp)
				.map(map -> {
					Serializable[] finalVals = Arrays.copyOf(vals, vals.length);
					for (int i = 0; i < comps.length; i++)
					{
						// fill a copy of the template 
						DataStructureComponent<?, ?, ?> c = comps[i];
						if (!ids.contains(c))
							finalVals[i] = map.get(c).get();
					}
					return (Row) new GenericRowWithSchema(finalVals, schema);
				}).iterator();
		}, resultEncoder.getRowEncoder());
		
		return new SparkDataSet(session, metadata, resultEncoder, flattenedDf);
	}
	
	@Override
	public DataSet filteredMappedJoin(DataSetMetadata metadata, DataSet other, SerBiPredicate<DataPoint, DataPoint> predicate, SerBinaryOperator<DataPoint> mergeOp, boolean leftJoin)
	{
		SparkDataSet sparkOther = other instanceof SparkDataSet ? ((SparkDataSet) other) : new SparkDataSet(session, other.getMetadata(), other);

		List<String> commonIDs = getMetadata().getIDs().stream()
				.filter(other.getMetadata()::contains)
				.map(DataStructureComponent::getVariable).map(Variable::getName)
				.collect(toList());

		Dataset<Row> leftDataframe = dataFrame.as("a");
		Dataset<Row> rightDataframe = sparkOther.dataFrame.as("b");
		int leftSize = getMetadata().size();
		
		// TODO: Try to execute filter with a null datapoint?
		FilterFunction<Row> sparkFilter = row -> {

			DataPoint right = leftJoin && row.get(leftSize + 1) == null ? null : sparkOther.encoder.decode(row, leftSize + 1);
			return predicate.test(encoder.decode(row), right);
		};
		
		Column joinKeys = null;
		for (String commonID: commonIDs)
		{
			Column newCond = leftDataframe.col(commonID).equalTo(rightDataframe.col(commonID)); 
			joinKeys = joinKeys == null ? newCond : joinKeys.and(newCond);
		}

		DataPointEncoder newEncoder = new DataPointEncoder(metadata);
		MapPartitionsFunction<Row, Row> sparkMerge = iterator -> new Iterator<Row>() {
				@Override
				public boolean hasNext()
				{
					return iterator.hasNext();
				}

				@Override
				public Row next()
				{
					Row row = iterator.next();
					DataPoint right = leftJoin && row.get(leftSize + 1) == null ? null : sparkOther.encoder.decode(row, leftSize + 1);
					return newEncoder.encode(mergeOp.apply(encoder.decode(row), right));
				}
			};
		
		Dataset<Row> joined; 
		if (commonIDs.isEmpty())
			joined = leftDataframe.crossJoin(rightDataframe);
		else
			joined = leftDataframe.join(rightDataframe, joinKeys, leftJoin ? "left" : "inner");
		
		if (!predicate.equals(DataSet.ALL))
			joined = joined.filter(sparkFilter);
		
		return new SparkDataSet(session, metadata, joined.mapPartitions(sparkMerge, newEncoder.getRowEncoder()));
	}

	
	@Override
	public <T, TT> DataSet analytic(SerFunction<DataPoint, Lineage> lineageOp, DataStructureComponent<?, ?, ?> sourceComp,
			DataStructureComponent<?, ?, ?> destComp, WindowClause clause, SerFunction<DataPoint, T> extractor, 
			SerCollector<T, ?, TT> collector2, SerBiFunction<TT, T, Collection<ScalarValue<?, ?, ?, ?>>> finisher)
	{
		// Convert a VTL window clause to a Spark Window Specification
		WindowSpec windowSpec = null;
		if (!clause.getPartitioningIds().isEmpty())
			windowSpec = Window.partitionBy(clause.getPartitioningIds().stream()
					.map(DataStructureComponent::getVariable).map(Variable::getName)
					.map(dataFrame::col)
					.collect(toArray(new Column[clause.getPartitioningIds().size()])));

		Column[] orderBy = clause.getSortCriteria().stream()
				.map(item -> {
					Column col = dataFrame.col(item.getComponent().getVariable().getName());
					return item.getMethod() == ASC ? col.asc() : col.desc();
				}).collect(toArray(new Column[clause.getSortCriteria().size()]));

		if (!clause.getSortCriteria().isEmpty())
			windowSpec = windowSpec == null ? Window.orderBy(orderBy) : windowSpec.orderBy(orderBy);
		
		LimitCriterion infBound = clause.getWindowCriterion().getInfBound(),
				supBound = clause.getWindowCriterion().getSupBound();
		long inf = infBound.getCount();  
		long sup = supBound.getCount();
		if (infBound.getDirection() == PRECEDING)
			inf = inf == Long.MAX_VALUE ? Long.MIN_VALUE : -inf;
		if (supBound.getDirection() == PRECEDING)
			sup = sup == Long.MAX_VALUE ? Long.MIN_VALUE : -sup;
			
		if (clause.getWindowCriterion().getType() == RANGE)
			windowSpec = windowSpec == null ? Window.rangeBetween(inf, sup) : windowSpec.rangeBetween(inf, sup);
		else
			windowSpec = windowSpec == null ? Window.rowsBetween(inf, sup) : windowSpec.rowsBetween(inf, sup);
		WindowSpec finalWindowSpec = windowSpec == null ? Window$.MODULE$.spec() : windowSpec;

		SerCollector<Row, ?, TT> collector;
		if (extractor == null)
			collector = (SerCollector<Row, ?, TT>) collector2;
		else
			collector = mapping(extractor.compose(encoder::decode), collector2);

		Serializable accum = (Serializable) collector.supplier().get();
		@SuppressWarnings("unchecked")
		Serializable tag = ((SerFunction<Object, Serializable>) collector.finisher()).apply(accum);
		
		DataSetMetadata structure = getMetadata();
		ValueDomainSubset<?, ?> domain = sourceComp.getVariable().getDomain();
		Encoder<?> accEncoder = SparkUtils.getEncoderFor(accum, domain, structure);
		@SuppressWarnings("unchecked")
		Encoder<TT> ttEncoder = (Encoder<TT>) SparkUtils.getEncoderFor(tag, domain, structure);
		
		Column[] columns = new Column[getMetadata().size() + 1];
		int l = 0;
		for (String col: dataFrame.columns())
			columns[l++] = dataFrame.col(col);
		
		Column udaf, input;
		UDF2<Serializable, ? extends Serializable, Serializable[]> udf;
		if (extractor == null) // then T is ScalarValue<?, ?, ?, ?>
		{
			Encoder<T> inputEncoder = (Encoder<T>) getEncoderFor(NullValue.instance(NULLDS), domain, structure);
			udaf = udaf(new VTLSparkAggregator<>(collector2, accEncoder, ttEncoder), inputEncoder)
					.apply(dataFrame.col(sourceComp.getVariable().getName()))
					.over(finalWindowSpec);
			udf = nonExtractingUDF(sourceComp, finisher);
			input = dataFrame.col(sourceComp.getVariable().getName());
		}
		else
		{
			udaf = udaf(new VTLSparkAggregator<>(collector, accEncoder, ttEncoder), encoder.rowEncoder)
					.apply(columns)
					.over(finalWindowSpec);
			udf = extractorUDF(sourceComp, extractor, finisher);
			input = struct(columns);
		}
		
		String destName = destComp.getVariable().getName();
		ArrayType arrayType = createArrayType(getDataTypeFor(sourceComp));
		Dataset<Row> analyticResult = dataFrame.withColumn(destName, udf(udf, arrayType).apply(udaf, input));
		Dataset<Row> exploded = analyticResult.withColumn(destName, explode(analyticResult.col(destName)));
		
		// Structure of the result
		DataSetMetadata newStructure = new DataStructureBuilder(getMetadata())
				.removeComponent(sourceComp)
				.addComponent(destComp)
				.build();
		
		DataPointEncoder resultEncoder = new DataPointEncoder(newStructure);
		Column[] cols = getColumnsFromComponents(newStructure).toArray(new Column[newStructure.size() + 1]);
		cols[cols.length - 1] = exploded.col("$lineage$");

		return new SparkDataSet(session, newStructure, resultEncoder, exploded.select(cols));
	}

	private <TT, T> UDF2<Serializable, Row, Serializable[]> extractorUDF(DataStructureComponent<?, ?, ?> comp, SerFunction<DataPoint, T> extractor, SerBiFunction<TT, T, Collection<ScalarValue<?, ?, ?, ?>>> finisher)
	{
		return (serAcc, input) -> {
			serAcc = SparkUtils.reinterpret(comp, serAcc);
			
			Collection<ScalarValue<?, ?, ?, ?>> finished;
			if (finisher != null)
				finished = finisher.apply((TT) serAcc, extractor.apply(encoder.decode(input)));
			else
				finished = List.of((ScalarValue<?, ?, ?, ?>) serAcc);
			
			return finished.stream()
					.map(ScalarValue::get)
					.collect(toArray(new Serializable[finished.size()]));
		};
	}

	private <TT, T> UDF2<Serializable, Serializable, Serializable[]> nonExtractingUDF(DataStructureComponent<?, ?, ?> comp, SerBiFunction<TT, T, Collection<ScalarValue<?, ?, ?, ?>>> finisher)
	{
		return (serAcc, input) -> {
			serAcc = SparkUtils.reinterpret(comp, serAcc);
			input = SparkUtils.reinterpret(comp, input);
			
			Collection<ScalarValue<?, ?, ?, ?>> finished;
			if (finisher != null)
				finished = finisher.apply((TT) serAcc, (T) input);
			else
				finished = List.of((ScalarValue<?, ?, ?, ?>) serAcc);
			
			return finished.stream()
					.map(ScalarValue::get)
					.collect(toArray(new Serializable[finished.size()]));
		};
	}

	@Override
	public <T extends Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>> DataSet aggregate(DataSetMetadata structure, 
			Set<DataStructureComponent<Identifier, ?, ?>> keys, SerCollector<DataPoint, ?, T> groupCollector,
			SerBiFunction<T, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, DataPoint> finisher)
	{
		DataPointEncoder resultEncoder = new DataPointEncoder(structure);
		int bufferSize = Integer.parseInt(VTL_SPARK_PAGE_SIZE.getValue());
		Dataset<Row> aggred;
		
		if (keys.isEmpty())
		{
			MapGroupsFunction<Integer, Row, Row> aggregator = (keyRow, s) -> StreamSupport.stream(new SparkSpliterator(s, bufferSize), !Utils.SEQUENTIAL)
				.map(encoder::decode)
				.collect(collectingAndThen(groupCollector, r -> resultEncoder.encode(finisher.apply(r, emptyMap()))));
			
			aggred = dataFrame.groupBy(lit(1))
					.as(INT(), encoder.getRowEncoder())
					.mapGroups(aggregator, resultEncoder.getRowEncoder());
		}
		else
		{
			DataPointEncoder keyEncoder = new DataPointEncoder(keys);
			Column[] keyNames = new Column[keys.size()];
			for (int i = 0; i < keyNames.length; i++)
				keyNames[i] = dataFrame.col(keyEncoder.components[i].getVariable().getName());
			
			MapGroupsFunction<Row, Row, Row> aggregator = (keyRow, s) -> {
					Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> keyValues = keys.stream()
						.collect(toMapWithValues(c -> getScalarFor(c, keyRow.getAs(c.getVariable().getName()))));
					return StreamSupport.stream(new SparkSpliterator(s, bufferSize), !Utils.SEQUENTIAL)
							.map(encoder::decode)
							.collect(collectingAndThen(groupCollector, r -> resultEncoder.encode(finisher.apply(r, keyValues))));
				};
			
			aggred = dataFrame.groupBy(keyNames)
					.as(keyEncoder.getRowEncoderNoLineage(), encoder.getRowEncoder())
					.mapGroups(aggregator, resultEncoder.getRowEncoder());
		}
		
		return new SparkDataSet(session, structure, aggred);
	}
	
	@Override
	public <A, T, TT> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys,
			Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> filter,
			SerCollector<DataPoint, A, TT> groupCollector,
			SerBiFunction<? super TT, ? super Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, T> finisher)
	{
		@SuppressWarnings("unchecked")
		DataStructureComponent<Identifier, ?, ?>[] sortedKeys = (DataStructureComponent<Identifier, ?, ?>[]) keys.stream()
				.sorted(DataStructureComponent::byName)
				.collect(toArray(new DataStructureComponent<?, ?, ?>[keys.size()]));
				
		Column[] groupingCols = keys.stream()
				.sorted(DataStructureComponent::byName)
				.map(DataStructureComponent::getVariable).map(Variable::getName)
				.map(functions::col)
				.collect(toArray(new Column[keys.size()]));
		
		// The encoder for the keys
		List<StructField> idFields = new ArrayList<>(createStructFromComponents(keys));
		Encoder<Row> keyEncoder = Encoders.row(new StructType(idFields.toArray(new StructField[keys.size()])));
		
		// Compute a single item to determine the encoder for type T
		List<DataPoint> sample = dataFrame.limit(1).collectAsList().stream().map(encoder::decode).collect(toList());
		T sampleResult = finisher.apply(sample.stream().collect(groupCollector), sample.get(0).getValues(keys, Identifier.class));
		
		if (sampleResult instanceof List && !((List<?>) sampleResult).isEmpty() && 
				(((List<?>) sampleResult).get(0) instanceof DataPoint))
			return processAsList(groupCollector, finisher, sortedKeys, groupingCols, keyEncoder);
		else if (sampleResult instanceof SortedMap && !((SortedMap<?, ?>) sampleResult).isEmpty() && 
				((SortedMap<?, ?>) sampleResult).firstKey() instanceof DataPoint)
		{
			@SuppressWarnings("unchecked")
			Comparator<DataPoint> comparator = (Comparator<DataPoint>) ((SortedMap<?, ?>) sampleResult).comparator();
			return processAsBoolMap(groupCollector, finisher, sortedKeys, groupingCols, keyEncoder, comparator);
		}
		else
			// Other cases not supported
			throw new UnsupportedOperationException(sampleResult.getClass().getSimpleName() + " not supported in Spark datasets");
	}

	private <A, TT, T> Stream<T> processAsList(SerCollector<DataPoint, A, TT> groupCollector,
			SerBiFunction<? super TT, ? super Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, T> finisher, 
			DataStructureComponent<Identifier, ?, ?>[] sortedKeys, Column[] groupingCols, Encoder<Row> keyEncoder)
	{
		LOGGER.warn("An unsupported transformation will move data into the driver. OutOfMemoryError may occur.");
		
		// case: supports decoding into a List<DataPoint> for fill_time_series
		List<DataStructureComponent<?, ?, ?>> resultComponents = getMetadata().stream()
				.sorted(DataStructureComponent::byNameAndRole)
				.collect(toList());
		
		// Use kryo encoder hoping that the class has been registered beforehand
		Encoder<Serializable[][]> resultEncoder = Encoders.kryo(Serializable[][].class);
		
		Dataset<Serializable[][]> result = dataFrame.groupBy(groupingCols).as(keyEncoder, encoder.getRowEncoder())
			.mapGroups(groupToListMapper(groupCollector, finisher, sortedKeys, encoder), resultEncoder);
		
		return StreamSupport.stream(spliteratorUnknownSize(result.toLocalIterator(), 0), !Utils.SEQUENTIAL)
				// decode Row[] from the UDF into List<DataPoint>
				.map(group -> Arrays.stream(group)
					.map(array -> IntStream.range(0, array.length - 1)
						.mapToObj(i -> new SimpleEntry<>(resultComponents.get(i), getScalarFor(resultComponents.get(i), array[i])))
						.collect(toDataPoint((Lineage) array[array.length - 1], getMetadata())))
					.collect(toList()))
				.map(out -> (T) out);
	}

	private <A, TT, T> Stream<T> processAsBoolMap(SerCollector<DataPoint, A, TT> groupCollector,
			SerBiFunction<? super TT, ? super Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, T> finisher, 
			DataStructureComponent<Identifier, ?, ?>[] sortedKeys, Column[] groupingCols, Encoder<Row> keyEncoder,
			Comparator<DataPoint> comparator)
	{
		LOGGER.warn("An unsupported transformation will move data into the driver. OutOfMemoryError may occur.");

		// case: supports decoding into a List<DataPoint> for fill_time_series
		List<DataStructureComponent<?, ?, ?>> resultComponents = getMetadata().stream()
				.sorted(DataStructureComponent::byNameAndRole)
				.collect(toList());

		// Use kryo encoder hoping that the class has been registered beforehand
		Encoder<Serializable[][]> resultEncoder = Encoders.kryo(Serializable[][].class);
		
		Dataset<Serializable[][]> result = dataFrame.groupBy(groupingCols).as(keyEncoder, encoder.getRowEncoder())
			.mapGroups(groupToSkipListMapper(groupCollector, finisher, sortedKeys, encoder), resultEncoder);
		
		// Rebuild a stream of ConcurrentSkipListMap
		return StreamSupport.stream(spliteratorUnknownSize(result.toLocalIterator(), 0), !Utils.SEQUENTIAL)
				// decode Row[] from the UDF into ConcurrentSkipListMap<DataPoint, ?>
				.map(group -> Arrays.stream(group)
						.map(array -> IntStream.range(0, array.length - 1)
							.mapToObj(i -> new SimpleEntry<>(resultComponents.get(i), getScalarFor(resultComponents.get(i), array[i])))
							.collect(toDataPoint((Lineage) array[array.length - 1], getMetadata())))
						.collect(toConcurrentMap(identity(), k -> TRUE, (a, b) -> a, () -> new ConcurrentSkipListMap<>(comparator))))
					.map(out -> (T) out);
	}

	@Override
	public DataSet union(SerFunction<DataPoint, Lineage> lineageOp, List<DataSet> others)
	{
		List<Dataset<Row>> allSparkDs = Stream.concat(Stream.of(this), others.stream())
				.map(other -> other instanceof SparkDataSet ? ((SparkDataSet) other) : new SparkDataSet(session, other.getMetadata(), other))
				.map(sds -> sds.dataFrame)
				.collect(toList());
		
		List<String> names = getNamesFromComponents(getMetadata());
		
		Column[][] allColumns = new Column[allSparkDs.size()][];
		IntStream.range(0, allSparkDs.size())
			.forEach(i -> {
				allColumns[i] = new Column[names.size()];
				IntStream.range(0, names.size())
					.forEach(j -> allColumns[i][j] = allSparkDs.get(i).col(names.get(j)));
			});
		
		// left-to-right union of all datasets and deletion of all duplicates
		Dataset<Row> result = IntStream.range(0, allSparkDs.size())
			.mapToObj(i -> allSparkDs.get(i).select(allColumns[i]).withColumn("__index", lit(i)))
			.reduce(Dataset::unionAll)
			.get();
		
		// remove duplicates and add lineage
		Column[] ids = getColumnsFromComponents(getMetadata().getIDs()).toArray(new Column[0]);
		Column[] cols = getColumnsFromComponents(getMetadata()).toArray(new Column[getMetadata().size()]);
		Column lineage = new Column(Literal.create(LineageSparkUDT.serialize(LineageExternal.of("Union")), LineageSparkUDT));
		result = result.withColumn("__index", first("__index").over(partitionBy(ids).orderBy(result.col("__index"))))
				.drop("__index")
				.select(cols)
				.withColumn("$lineage$", lineage);
		
		return new SparkDataSet(session, getMetadata(), encoder, result);
	}

	private static <TT, A, T> MapGroupsFunction<Row, Row, Serializable[][]> groupToListMapper(SerCollector<DataPoint, A, TT> groupCollector,
			SerBiFunction<? super TT, ? super Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, T> finisher,
			DataStructureComponent<Identifier, ?, ?>[] sortedKeys, DataPointEncoder encoder)
	{
		return (keyRow, values) -> {
			TT before = StreamSupport.stream(spliteratorUnknownSize(values, ORDERED), !Utils.SEQUENTIAL)
				.map(encoder::decode)
				.collect(groupCollector);

			Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> keyMap = new HashMap<>();
			for (int i = 0; i < keyRow.size(); i++)
				keyMap.put(sortedKeys[i], getScalarFor(sortedKeys[i], (Serializable) keyRow.get(i)));
			
			// Each group is mapped to an array of rows where each row is an array of values
			Serializable[][] array = ((Collection<?>) finisher.apply(before, keyMap)).stream()
				.map(DataPoint.class::cast)
				.map(encoder::encode)
				.map(row -> {
					int size = row.length();
					Serializable[] arrayRow = new Serializable[size];
					for (int i = 0; i < size; i++)
						arrayRow[i] = (Serializable) row.get(i);
					return arrayRow;
				}).collect(collectingAndThen(toList(), l -> l.toArray(new Serializable[l.size()][])));
			
			return array;
		};
	}

	private static <TT, A, T> MapGroupsFunction<Row, Row, Serializable[][]> groupToSkipListMapper(SerCollector<DataPoint, A, TT> groupCollector,
			SerBiFunction<? super TT, ? super Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, T> finisher,
			DataStructureComponent<Identifier, ?, ?>[] sortedKeys, DataPointEncoder encoder)
	{
		return (keyRow, values) -> {
			TT before = StreamSupport.stream(spliteratorUnknownSize(values, ORDERED), !Utils.SEQUENTIAL)
				.map(encoder::decode)
				.collect(groupCollector);

			Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> keyMap = new HashMap<>();
			for (int i = 0; i < keyRow.size(); i++)
				keyMap.put(sortedKeys[i], getScalarFor(sortedKeys[i], (Serializable) keyRow.get(i)));
			
			// Each group is mapped to an array of rows where each row is an array of values
			Serializable[][] array = ((ConcurrentSkipListMap<?, ?>) finisher.apply(before, keyMap)).keySet().stream()
					.map(DataPoint.class::cast)
					.map(encoder::encode)
					.map(row -> {
						int size = row.length();
						Serializable[] arrayRow = new Serializable[size];
						for (int i = 0; i < size; i++)
							arrayRow[i] = (Serializable) row.get(i);
						return arrayRow;
					}).collect(collectingAndThen(toList(), l -> l.toArray(new Serializable[l.size()][])));
			
			return array;
		};
	}

	private static List<Row> collectToDF(DataSet toWrap)
	{
		final DataSetMetadata dataStructure = toWrap.getMetadata();
		DataPointEncoder encoder = new DataPointEncoder(toWrap.getMetadata());

		LOGGER.info("Loading data into Spark dataframe...", dataStructure);

		try (Stream<DataPoint> stream = toWrap.stream())
		{
			return stream.map(encoder::encode).collect(toList());
		}
	}

	private void logInfo(String description)
	{
		LOGGER.info("Spark execution plan prepared for {}", description);
		LOGGER.trace("{}", dataFrame.queryExecution().explainString(ExplainMode.fromString("formatted")));
	}
}
