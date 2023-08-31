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

import static it.bancaditalia.oss.vtl.impl.environment.spark.DataPointEncoder.createStructFromComponents;
import static it.bancaditalia.oss.vtl.impl.environment.spark.DataPointEncoder.getColumnsFromComponents;
import static it.bancaditalia.oss.vtl.impl.environment.spark.DataPointEncoder.getDataTypeForComponent;
import static it.bancaditalia.oss.vtl.impl.environment.spark.DataPointEncoder.getEncoderForComponent;
import static it.bancaditalia.oss.vtl.impl.environment.spark.DataPointEncoder.getNamesFromComponents;
import static it.bancaditalia.oss.vtl.impl.environment.spark.DataPointEncoder.scalarFromColumnValue;
import static it.bancaditalia.oss.vtl.impl.environment.spark.DataPointEncoder.sorter;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkEnvironment.LineageSparkUDT;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.toDataPoint;
import static it.bancaditalia.oss.vtl.model.data.DataStructureComponent.byName;
import static it.bancaditalia.oss.vtl.model.transform.analytic.LimitCriterion.LimitDirection.PRECEDING;
import static it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion.SortingMethod.ASC;
import static it.bancaditalia.oss.vtl.model.transform.analytic.WindowCriterion.LimitType.RANGE;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.mapping;
import static it.bancaditalia.oss.vtl.util.SerCollectors.teeing;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toArray;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithValues;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.spark.sql.expressions.Window.partitionBy;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.udaf;
import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.types.DataTypes.createArrayType;
import static scala.collection.JavaConverters.asScalaBuffer;

import java.io.Serializable;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.spark.api.java.function.FilterFunction;
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
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.execution.ExplainMode;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.Window$;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

import it.bancaditalia.oss.vtl.impl.types.data.date.DayHolder;
import it.bancaditalia.oss.vtl.impl.types.dataset.AbstractDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLInvariantIdentifiersException;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageExternal;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.transform.analytic.LimitCriterion;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowClause;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerBiPredicate;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerFunction;
import it.bancaditalia.oss.vtl.util.SerPredicate;
import it.bancaditalia.oss.vtl.util.Utils;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;

public class SparkDataSet extends AbstractDataSet
{
	private static final Logger LOGGER = LoggerFactory.getLogger(SparkDataSet.class);
	private static final long serialVersionUID = 1L;

	private final SparkSession session;
	private final DataPointEncoder encoder;
	private final Dataset<Row> dataFrame;
	
	public SparkDataSet(SparkSession session, DataPointEncoder encoder, Dataset<Row> dataFrame)
	{
		super(encoder.getStructure());
		
		this.session = session;
		this.encoder = encoder;
		this.dataFrame = dataFrame;

		logInfo(toString());
	}

	public SparkDataSet(SparkSession session, DataSetMetadata dataStructure, Dataset<Row> dataFrame)
	{
		this(session, new DataPointEncoder(dataStructure), dataFrame);
	}

	public SparkDataSet(SparkSession session, DataSetMetadata dataStructure, DataSet toWrap)
	{
		this(session, dataStructure, session.createDataset(collectToDF(toWrap), 
				new DataPointEncoder(toWrap.getMetadata()).getRowEncoder()).cache());
	}
	
	@Override
	protected Stream<DataPoint> streamDataPoints()
	{
		int bufferSize = Integer.parseInt(SparkEnvironment.VTL_SPARK_PAGE_SIZE.getValue());
		BlockingQueue<Row> queue = new ArrayBlockingQueue<>(bufferSize);
		
		LOGGER.warn("A trasformation is moving data from Spark into the driver. OutOfMemoryError may occur.");
		
		Thread pushingThread = new Thread(() -> {
			LOGGER.info("Getting datapoints from Spark...");
			try
			{
				Iterator<Row> iterator = dataFrame.toLocalIterator();
				if (LOGGER.isTraceEnabled())
					LOGGER.trace("{}", dataFrame.queryExecution().explainString(ExplainMode.fromString("formatted")));
				while (iterator.hasNext())
					try
					{
						Row row = iterator.next();
						while (!queue.offer(row, 200, MILLISECONDS))
							;
					}
					catch (InterruptedException e)
					{
						LOGGER.error("Interrupted", e);
						Thread.currentThread().interrupt();
						break;
					}
			}
			catch (Exception e)
			{
				LOGGER.error("Error while getting datapoints from Spark", e);
			}
		}, "VTL Spark retriever");
		pushingThread.setDaemon(true);
		pushingThread.start();

		return StreamSupport.stream(new SparkSpliterator(queue, pushingThread), !Utils.SEQUENTIAL)
				.map(encoder::decode);
	}
	
	@Override
	public boolean isCacheable()
	{
		return false;
	}

	/*package*/ Dataset<Row> getDataFrame()
	{
		return dataFrame;
	}

	@Override
	public DataSet membership(String alias, Lineage lineage)
	{
		final DataSetMetadata membershipStructure = getMetadata().membership(alias);
		LOGGER.debug("Creating dataset by membership on {} from {} to {}", alias, getMetadata(), membershipStructure);
		
		DataStructureComponent<? extends NonIdentifier, ?, ?> membershipMeasure = membershipStructure.getComponents(Measure.class).iterator().next();
		
		Dataset<Row> newDF = dataFrame;
		if (!getMetadata().contains(membershipMeasure))
			newDF = dataFrame.withColumnRenamed(alias, membershipMeasure.getName());
		
		Column[] columns = getColumnsFromComponents(newDF, membershipStructure).toArray(new Column[membershipStructure.size()]);

		// order columns alphabetically
		newDF = newDF.select(columns);
		
		byte[] serializedLineage = LineageSparkUDT.serialize(lineage);
		return new SparkDataSet(session, membershipStructure, newDF.withColumn("$lineage$", lit(serializedLineage)));
	}

	@Override
	public DataSet filter(SerPredicate<DataPoint> predicate,
			SerFunction<? super DataPoint, ? extends Lineage> lineageOperator)
	{
		FilterFunction<Row> func = row -> predicate.test(encoder.decode(row));
		DataSetMetadata structure = getMetadata();
		MapFunction<Row, Row> lineage = row -> {
			DataPoint dp = encoder.decode(row);
			DataPoint newDp = new DataPointBuilder(dp).build(lineageOperator.apply(dp), structure);
			return encoder.encode(newDp);
		};
		return new SparkDataSet(session, getMetadata(), dataFrame.filter(func).map(lineage, encoder.getRowEncoder()));
	}
	
	@Override
	public DataSet getMatching(Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> keyValues)
	{
		return filter(dp -> keyValues.equals(dp.getValues(keyValues.keySet(), Identifier.class)), DataPoint::getLineage);	
	}
	
	@Override
	public long size()
	{
		return dataFrame.count();
	}

	@Override
	public DataSet mapKeepingKeys(DataSetMetadata metadata, SerFunction<? super DataPoint, ? extends Lineage> lineageOperator,
			SerFunction<? super DataPoint, ? extends Map<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>>> operator)
	{
		final Set<DataStructureComponent<Identifier, ?, ?>> originalIDs = getMetadata().getComponents(Identifier.class);
		if (!metadata.getComponents(Identifier.class).containsAll(originalIDs))
			throw new VTLInvariantIdentifiersException("map", originalIDs, metadata.getComponents(Identifier.class));
		
		LOGGER.trace("Creating dataset by mapping from {} to {}", getMetadata(), metadata);
		
		// wrap the result of the mapping of the decoded datapoint into a struct
		UDF1<Row, Row> wrappedOperator = row -> {
			DataPoint dp = encoder.decode(row);
			Map<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> map = operator.apply(dp);
			map.keySet().removeAll(originalIDs);
			
			// compute values
			Object[] resultArray = map.entrySet().stream()
					.sorted((a, b) -> sorter(a.getKey(), b.getKey()))
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
		fields.add(new StructField("$lineage$", LineageSparkUDT, false, null));
		StructType structType = new StructType(fields.toArray(new StructField[fields.size()]));
		
		// wrap the source row into a struct for passing to the UDF
		Column wrappedRow = Arrays.stream(dataFrame.columns())
			.map(dataFrame::col)
			.collect(collectingAndThen(collectingAndThen(toList(), 
					JavaConverters::asScalaBuffer),
					functions::struct));
		
		// the columns to keep
		String[] ids = originalIDs.stream()
				.map(DataStructureComponent::getName)
				.collect(toArray(new String[originalIDs.size()]));

		// apply the transformation to the data frame and unwrap the result
		Dataset<Row> dataFrame2 = dataFrame.withColumn("__extended", udf(wrappedOperator, structType).apply(wrappedRow))
				.select("__extended.*", ids);
		
		// reorder columns to match the encoder order
		Column[] ordered = getColumnsFromComponents(dataFrame2, metadata).toArray(new Column[metadata.size() + 1]);
		ordered[ordered.length - 1] = dataFrame2.col("$lineage$");

		return new SparkDataSet(session, metadata, dataFrame2.select(ordered));
	}

	@Override
	public DataSet filteredMappedJoin(DataSetMetadata metadata, DataSet other, SerBiPredicate<DataPoint, DataPoint> predicate, SerBinaryOperator<DataPoint> mergeOp, boolean leftJoin)
	{
		SparkDataSet sparkOther = other instanceof SparkDataSet ? ((SparkDataSet) other) : new SparkDataSet(session, other.getMetadata(), other);

		List<String> commonIDs = getComponents(Identifier.class).stream()
				.filter(other.getMetadata()::contains)
				.map(DataStructureComponent::getName)
				.collect(toList());
		
		Dataset<Row> leftDataframe = dataFrame.as("a");
		Dataset<Row> rightDataframe = sparkOther.dataFrame.as("b");
		for (String commonID: commonIDs)
		{
			leftDataframe = leftDataframe.withColumnRenamed(commonID, "a_" + commonID);
			rightDataframe = rightDataframe.withColumnRenamed(commonID, "b_" + commonID);
		}
		
		Dataset<Row> finalLeft = leftDataframe;
		Dataset<Row> finalRight = rightDataframe;

		Column joinKeys = commonIDs.stream()
				.map(n -> finalLeft.col("a_" + n).equalTo(finalRight.col("b_" + n)))
				.reduce(Column::and)
				.get();

		// TODO: Try to execute filter with a null datapoint?
		FilterFunction<Tuple2<Row, Row>> sparkFilter = rows -> {
			final DataPoint left = encoder.decode(rows._1);
			if (leftJoin && IntStream.range(0, rows._2.length()).allMatch(i -> rows._2.get(i) == null))
				return true;
			else
				return predicate.test(left, sparkOther.encoder.decode(rows._2));
		};
		
		DataPointEncoder newEncoder = new DataPointEncoder(metadata);
		MapPartitionsFunction<Tuple2<Row, Row>, Row> sparkMerge = iterator -> new Iterator<Row>() {
				@Override
				public boolean hasNext()
				{
					return iterator.hasNext();
				}

				@Override
				public Row next()
				{
					Tuple2<Row, Row> rows = iterator.next();
					final DataPoint left = encoder.decode(rows._1);
					DataPoint result;
					if (leftJoin && IntStream.range(0, rows._2.length()).allMatch(i -> rows._2.get(i) == null))
						result = mergeOp.apply(left, null);
					else
						result = mergeOp.apply(left, sparkOther.encoder.decode(rows._2));
					
					return newEncoder.encode(result);
				}
			};
		
		Dataset<Row> joined = finalLeft.joinWith(finalRight, joinKeys, leftJoin ? "left" : "inner")
				.filter(sparkFilter)
				.mapPartitions(sparkMerge, newEncoder.getRowEncoder());
		
		return new SparkDataSet(session, metadata, joined);
	}

	@Override
	public <TT> DataSet analytic(SerFunction<DataPoint, Lineage> lineageOp, 
			Map<? extends DataStructureComponent<?, ?, ?>, ? extends DataStructureComponent<?, ?, ?>> components,
			WindowClause clause,
			Map<? extends DataStructureComponent<?, ?, ?>, SerCollector<ScalarValue<?, ?, ?, ?>, ?, TT>> collectors,
			Map<? extends DataStructureComponent<?, ?, ?>, SerBiFunction<TT, ScalarValue<?, ?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>>> finishers)
	{
		// Convert a VTL window clause to a Spark Window Specification
		WindowSpec windowSpec = null;
		if (!clause.getPartitioningIds().isEmpty())
			windowSpec = Window.partitionBy(clause.getPartitioningIds().stream()
					.map(DataStructureComponent::getName)
					.map(dataFrame::col)
					.collect(toArray(new Column[clause.getPartitioningIds().size()])));

		Column[] orderBy = clause.getSortCriteria().stream()
				.map(item -> {
					Column col = dataFrame.col(item.getComponent().getName());
					return item.getMethod() == ASC ? col.asc() : col.desc();
				}).collect(toArray(new Column[clause.getPartitioningIds().size()]));

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
		
		
		// Create a window function for each component listed in the map
		Map<DataStructureComponent<?, ?, ?>, VTLSparkAggregator<?>> udafs = new HashMap<>();
		for (DataStructureComponent<?, ?, ?> oldComp: components.keySet())
				udafs.put(oldComp, new VTLSparkAggregator<>(oldComp, components.get(oldComp), collectors.get(oldComp), session));

		DataStructureComponent<?, ?, ?>[] sourceComponents = components.keySet().toArray(new DataStructureComponent<?, ?, ?>[0]);
		
		// Mapping from source component to its analytic function
		Map<DataStructureComponent<?, ?, ?>, Column> analytic = Arrays.stream(sourceComponents)
				.collect(toMapWithValues(comp -> {
					@SuppressWarnings("unchecked")
					// Safe as all scalars are Serializable
					Encoder<Serializable> measureEncoder = (Encoder<Serializable>) getEncoderForComponent(comp);
					Column column = dataFrame.col(comp.getName());
					Column udaf = udaf(udafs.get(comp), measureEncoder).apply(column).over(finalWindowSpec);
					
					// Apply the finisher (as an udf) to the result of the window function
					return udf((newV, oldV) -> udfForComponent(comp, newV, oldV, finishers.get(comp)), createArrayType(getDataTypeForComponent(comp)))
							.apply(udaf, column);
				}));
		
		
		// Invert the original components map
		Map<DataStructureComponent<?, ?, ?>, DataStructureComponent<?, ?, ?>> destToSrc = components.entrySet().stream()
				.sorted((e1, e2) -> sorter(e1.getValue(), e2.getValue()))
				.collect(toConcurrentMap(Entry::getValue, Entry::getKey));
		
		// create the column array for transformation
		Entry<List<String>, List<Column>> destComponents = destToSrc.entrySet().stream()
				.sorted((e1, e2) -> sorter(e1.getValue(), e2.getValue()))
				.map(e -> new SimpleEntry<>(e.getValue().getName(), analytic.get(e.getKey())))
				.collect(teeing(mapping(Entry::getKey, toList()), 
						mapping(Entry::getValue, toList()), 
						(l1, l2) -> new SimpleEntry<List<String>, List<Column>>(l1, l2)));
		
		// apply all the udfs
		Dataset<Row> withColumns = dataFrame.withColumns(asScalaBuffer(destComponents.getKey()), asScalaBuffer(destComponents.getValue()));
		
		// explode each column that is the result of the analytic invocation
		for (String name: destComponents.getKey())
			withColumns = withColumns.withColumn(name, explode(withColumns.col(name)));
		Dataset<Row> exploded = withColumns;
		
		// Structure of the result
		DataSetMetadata newStructure = new DataStructureBuilder(getMetadata())
				.removeComponents(components.keySet())
				.addComponents(components.values())
				.build();
		
		DataPointEncoder resultEncoder = new DataPointEncoder(newStructure);
		Column[] cols = getColumnsFromComponents(exploded, newStructure).toArray(new Column[newStructure.size() + 1]);
		cols[cols.length - 1] = exploded.col("$lineage$");

		return new SparkDataSet(session, resultEncoder, exploded.select(cols));
	}

	private <TT> Object[] udfForComponent(DataStructureComponent<?, ?, ?> comp, Object newV, Object oldV, 
			SerBiFunction<TT, ScalarValue<?, ?, ?, ?>, Collection<ScalarValue<?, ?, ?, ?>>> finisher)
	{
		// The result should be TT, but it must be constructed back if it isn't
		// ClassCastException may happen if the result type is not supported by spark
		Object result;
		if (newV instanceof byte[])
		{
			// This is the decoder for Encoders.kryo(ArrayList.class) used in VTLSparkAggregator
			final Kryo kryo = new Kryo();
			result = Arrays.stream((Serializable[]) kryo.readClassAndObject(new Input((byte[]) newV)))
					.map(v -> v instanceof byte[] ? kryo.readClassAndObject(new Input((byte[]) v)) : v) 
					.map(v -> scalarFromColumnValue(v, comp))
					.collect(toList());
		}
		else
			result = scalarFromColumnValue(newV, comp);
		
		Collection<ScalarValue<?, ?, ?, ?>> finished = finisher.apply((TT) result, scalarFromColumnValue(oldV, comp));
	
		return finished.stream()
				.map(ScalarValue::get)
				.map(v -> v instanceof DayHolder ? ((DayHolder) v).getLocalDate() : v)
				.collect(toArray(new Object[finished.size()]));
	}
	
	@Override
	public <TT> DataSet aggr(DataSetMetadata structure, Set<DataStructureComponent<Identifier, ?, ?>> keys,
			SerCollector<DataPoint, ?, TT> groupCollector,
			SerBiFunction<TT, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, DataPoint> finisher)
	{
		DataPointEncoder keyEncoder = new DataPointEncoder(keys);
		DataPointEncoder resultEncoder = new DataPointEncoder(structure);
		Column[] keyNames = keys.stream()
				.map(DataStructureComponent::getName)
				.map(dataFrame::col)
				.collect(toArray(new Column[keys.size()]));
		
		MapGroupsFunction<Row, Row, Row> aggregator = (keyRow, s) -> {
					Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> keyValues = keys.stream()
						.collect(toMapWithValues(c -> scalarFromColumnValue(keyRow.getAs(c.getName()), c)));
					return StreamSupport.stream(spliteratorUnknownSize(s, 0), !Utils.SEQUENTIAL)
							.map(encoder::decode)
							.collect(collectingAndThen(groupCollector, r -> resultEncoder.encode(finisher.apply(r, keyValues))));
				};
		
		Dataset<Row> grouped = dataFrame.groupBy(keyNames)
				.as(keyEncoder.getRowEncoderNoLineage(), encoder.getRowEncoder())
				.mapGroups(aggregator, resultEncoder.getRowEncoder());
		
		return new SparkDataSet(session, structure, grouped);
	}
	
	
	@Override
	public <A, T, TT> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys,
			Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> filter,
			SerCollector<DataPoint, A, TT> groupCollector,
			SerBiFunction<TT, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, T> finisher)
	{
		List<DataStructureComponent<Identifier, ?, ?>> sortedKeys = keys.stream()
				.sorted(byName())
				.collect(toList());

		Column[] groupingCols = getColumnsFromComponents(dataFrame, sortedKeys).toArray(new Column[sortedKeys.size()]);
		
		// The encoder for the keys
		List<StructField> idFields = new ArrayList<>(createStructFromComponents(keys));
		Encoder<Row> keyEncoder = RowEncoder.apply(new StructType(idFields.toArray(new StructField[keys.size()])));
		
		// Compute a single item to determine the encoder for type T
		List<DataPoint> sample = dataFrame.limit(1).collectAsList().stream().map(encoder::decode).collect(toList());
		T sampleResult = finisher.apply(sample.stream().collect(groupCollector), sample.get(0).getValues(keys, Identifier.class));

		if (sampleResult instanceof List && !((List<?>) sampleResult).isEmpty() && 
				(((List<?>) sampleResult).get(0) instanceof DataPoint))
		{
			// case: supports decoding into a List<DataPoint> for fill_time_series
			List<DataStructureComponent<?, ?, ?>> resultComponents = getMetadata().stream()
					.sorted(DataPointEncoder::sorter)
					.collect(toList());
			
			// Use kryo encoder hoping that the class has been registered beforehand
			Encoder<Serializable[][]> resultEncoder = Encoders.kryo(Serializable[][].class);
			
			Dataset<Serializable[][]> result = dataFrame.groupBy(groupingCols).as(keyEncoder, encoder.getRowEncoder())
				.mapGroups(groupMapper(groupCollector, finisher, sortedKeys, encoder), resultEncoder);
			
			LOGGER.warn("A trasformation is moving data from Spark into the driver. OutOfMemoryError may occur.");
			return StreamSupport.stream(spliteratorUnknownSize(result.toLocalIterator(), 0), !Utils.SEQUENTIAL)
					// decode Row[] from the UDF into List<DataPoint>
					.map(group -> Arrays.stream(group)
						.map(array -> IntStream.range(0, array.length - 1)
							.mapToObj(i -> new SimpleEntry<>(resultComponents.get(i), scalarFromColumnValue(array[i], resultComponents.get(i))))
							.collect(toDataPoint((Lineage) array[array.length - 1], getMetadata())))
						.collect(toList()))
					.map(out -> (T) out);
		}
		else
			// Other cases not supported
			throw new UnsupportedOperationException(sampleResult.getClass().getName() + " not supported in Spark datasets");
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
		Column[] ids = getColumnsFromComponents(result, getMetadata().getComponents(Identifier.class)).toArray(new Column[0]);
		Column[] cols = getColumnsFromComponents(result, getMetadata()).toArray(new Column[getMetadata().size()]);
		Column lineage = new Column(Literal.create(LineageSparkUDT.serialize(LineageExternal.of("Union")), LineageSparkUDT));
		result = result.withColumn("__index", first("__index").over(partitionBy(ids).orderBy(result.col("__index"))))
				.drop("__index")
				.select(cols)
				.withColumn("$lineage$", lineage);
		
		return new SparkDataSet(session, encoder, result);
	}
	
	@Override
	public DataSet setDiff(DataSet other)
	{
		SparkDataSet sparkOther = other instanceof SparkDataSet ? ((SparkDataSet) other) : new SparkDataSet(session, other.getMetadata(), other);
		List<String> ids = getMetadata().getComponents(Identifier.class).stream().map(DataStructureComponent::getName).collect(toList());
		Dataset<Row> result = dataFrame.join(sparkOther.dataFrame, asScalaBuffer(ids), "leftanti");

		Column[] cols = getColumnsFromComponents(result, getMetadata()).toArray(new Column[getMetadata().size() + 1]);
		cols[cols.length - 1] = result.col("$lineage$");
		
		return new SparkDataSet(session, encoder, result.select(cols));
	}

	private static <TT, A, T> MapGroupsFunction<Row, Row, Serializable[][]> groupMapper(SerCollector<DataPoint, A, TT> groupCollector,
			SerBiFunction<TT, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, T> finisher,
			List<DataStructureComponent<Identifier, ?, ?>> sortedKeys, DataPointEncoder encoder)
	{
		return (keyRow, values) -> {
			TT before = StreamSupport.stream(spliteratorUnknownSize(values, ORDERED), !Utils.SEQUENTIAL)
				.map(encoder::decode)
				.collect(groupCollector);

			Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> keyMap = new HashMap<>();
			for (int i = 0; i < keyRow.size(); i++)
				keyMap.put(sortedKeys.get(i), scalarFromColumnValue(keyRow.get(i), sortedKeys.get(i)));
			
			// Each group is mapped to an array of rows where each row is an array of values
			Serializable[][] array = ((Collection<?>) finisher.apply(before, keyMap)).stream()
				.map(DataPoint.class::cast)
				.map(encoder::encode)
				.map(row -> (Serializable[]) row.toSeq().toArray(ClassTag.apply(Serializable.class)))
				.collect(collectingAndThen(toList(), l -> l.toArray(new Serializable[l.size()][])));
			
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
