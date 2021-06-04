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

import static it.bancaditalia.oss.vtl.impl.environment.spark.DataPointEncoder.scalarFromColumnValue;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static org.apache.spark.sql.functions.lit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ExplainMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.types.dataset.AbstractDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLInvariantIdentifiersException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerBiPredicate;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerFunction;
import it.bancaditalia.oss.vtl.util.SerPredicate;
import it.bancaditalia.oss.vtl.util.Utils;

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
	}

	public SparkDataSet(SparkSession session, DataSetMetadata dataStructure, Dataset<Row> dataFrame)
	{
		super(dataStructure);
		this.session = session;
		this.encoder = new DataPointEncoder(dataStructure);
		this.dataFrame = dataFrame;
		
		LOGGER.info("Spark execution plan prepared for {}", dataStructure);
		LOGGER.trace("{}", dataFrame.queryExecution().explainString(ExplainMode.fromString("formatted")));
	}

	public SparkDataSet(SparkSession session, DataSetMetadata dataStructure, DataSet toWrap)
	{
		super(dataStructure);
		this.session = session;
		this.encoder = new DataPointEncoder(dataStructure);
		this.dataFrame = session.createDataFrame(toWrap.stream().map(encoder::encode).collect(toList()), encoder.getSchema());
	}

	@Override
	protected Stream<DataPoint> streamDataPoints()
	{
		BlockingQueue<Row> queue = new ArrayBlockingQueue<>(1000);
		AtomicBoolean finished = new AtomicBoolean(false);
		
		Thread pushingThread = new Thread(() -> {
			LOGGER.info("Getting datapoints from Spark...");
			try
			{
				Iterator<Row> i = dataFrame.cache().toLocalIterator();
				while (i.hasNext())
					try
					{
						Row row = i.next();
						while (!queue.offer(row, 100, MILLISECONDS))
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
			finally
			{
				finished.set(true);
			}
		});
		pushingThread.start();

		return StreamSupport.stream(new SparkSpliterator(queue, finished), !Utils.SEQUENTIAL)
				.map(encoder::decode);
	}

	@Override
	public DataSet membership(String alias, Lineage lineage)
	{
		final DataSetMetadata membershipStructure = getMetadata().membership(alias);
		LOGGER.debug("Creating dataset by membership on {} from {} to {}", alias, getMetadata(), membershipStructure);
		
		DataStructureComponent<? extends NonIdentifier, ?, ?> membershipMeasure = membershipStructure.getComponents(Measure.class).iterator().next();
		Set<DataStructureComponent<Identifier, ?, ?>> ids = getMetadata().getComponents(Identifier.class);
		
		Dataset<Row> newDF = dataFrame;
		if (!getMetadata().contains(membershipMeasure))
			newDF = dataFrame.withColumnRenamed(alias, membershipMeasure.getName());
		
		List<Column> columns = concat(Stream.of(membershipMeasure), ids.stream())
				.map(DataStructureComponent::getName)
				.sorted()
				.map(newDF::col)
				.collect(toCollection(ArrayList::new));
		// order columns alphabetically
		newDF = newDF.select(columns.toArray(new Column[columns.size()]));
		
		byte[] serializedLineage = LineageSparkUDT$.MODULE$.serialize(lineage);
		return new SparkDataSet(session, membershipStructure, newDF.withColumn("$lineage$", lit(serializedLineage)));
	}

	@Override
	public DataSet filter(SerPredicate<DataPoint> predicate)
	{
		return new SparkDataSet(session, getMetadata(), dataFrame.filter(encoder.wrapPredicate(predicate)));
	}

	@Override
	public DataSet getMatching(Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> keyValues)
	{
		return filter(dp -> keyValues.equals(dp.getValues(keyValues.keySet(), Identifier.class)));	
	}
	
	@Override
	public DataSet mappedJoin(DataSetMetadata metadata, DataSet indexed, SerBinaryOperator<DataPoint> merge)
	{
		return filteredMappedJoin(metadata, indexed, (a,  b) -> true, merge);
	}
	
	@Override
	public long size()
	{
		return dataFrame.count();
	}

	@Override
	public DataSet mapKeepingKeys(DataSetMetadata metadata,
			SerFunction<? super DataPoint, ? extends Lineage> lineageOperator,
			SerFunction<? super DataPoint, ? extends Map<? extends DataStructureComponent<? extends NonIdentifier, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>>> operator)
	{
		final Set<DataStructureComponent<Identifier, ?, ?>> identifiers = getMetadata().getComponents(Identifier.class);
		if (!metadata.getComponents(Identifier.class).equals(identifiers))
			throw new VTLInvariantIdentifiersException("map", identifiers, metadata.getComponents(Identifier.class));
		
		LOGGER.trace("Creating dataset by mapping from {} to {}", getMetadata(), metadata);
		
		MapFunction<DataPoint, DataPoint> extendingOperator = (MapFunction<DataPoint, DataPoint> & Serializable) 
				dp -> new DataPointBuilder(dp.getValues(Identifier.class))
				.addAll(operator.apply(dp))
				.build(lineageOperator.apply(dp), metadata, SparkDataPoint::new);
		
		DataPointEncoder newEncoder = new DataPointEncoder(metadata);
		return new SparkDataSet(session, metadata, dataFrame
				.map(newEncoder.wrapWithEncoder(encoder.wrapWithDecoder(extendingOperator)), newEncoder.getRowEncoder()));
	}
	
	@Override
	public DataSet filteredMappedJoin(DataSetMetadata metadata, DataSet other, SerBiPredicate<DataPoint, DataPoint> predicate, SerBinaryOperator<DataPoint> mergeOp)
	{
		SparkDataSet sparkOther = other instanceof SparkDataSet ? ((SparkDataSet) other) : new SparkDataSet(session, other.getMetadata(), other);
		
		/* the filter separately deserialize each of the two datapoints */
		AtomicInteger index = new AtomicInteger(-1);
		FilterFunction<Row> sparkFilter = row -> {
			// find the split column
			int indexUnboxed = index.updateAndGet(old -> {
				if (old >= 0)
					return old;
				int newIndex = 0;
				for (; newIndex < row.size(); newIndex++)
					if ("$lineage$".equals(row.schema().fieldNames()[newIndex]))
						break;
				return ++newIndex;
			});
			return predicate.test(encoder.decode(row), sparkOther.encoder.decode(row, indexUnboxed));
		};
				
		MapFunction<Row, DataPoint> sparkMerge = row -> mergeOp.apply(encoder.decode(row), sparkOther.encoder.decode(row, index.get()));
		
		Column commonIDs = Utils.getStream(getComponents(Identifier.class))
			.filter(other.getMetadata()::contains)
			.map(DataStructureComponent::getName)
			.map(name -> dataFrame.alias("a").col(name).equalTo(sparkOther.dataFrame.alias("b").col(name)))
			.reduce(Column::and)
			.get();
		
		DataPointEncoder newEncoder = new DataPointEncoder(metadata);
		Dataset<Row> joined = dataFrame.alias("a").join(sparkOther.dataFrame.alias("b"), commonIDs)
			.filter(sparkFilter)
			.map(newEncoder.wrapWithEncoder(sparkMerge), newEncoder.getRowEncoder());
		
		return new SparkDataSet(session, metadata, joined);
	}

	@Override
	public <TT> DataSet aggr(DataSetMetadata structure, Set<DataStructureComponent<Identifier, ?, ?>> keys,
			SerCollector<DataPoint, ?, TT> groupCollector,
			SerBiFunction<TT, Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, DataPoint> finisher)
	{
		DataPointEncoder keyEncoder = new DataPointEncoder(keys);
		DataPointEncoder resultEncoder = new DataPointEncoder(structure);
		Column[] keyNames = Utils.getStream(keys)
				.map(DataStructureComponent::getName)
				.map(dataFrame::col)
				.collect(toSet())
				.toArray(new Column[keys.size()]);
		
		MapGroupsFunction<Row, Row, Row> aggregator = (keyRow, s) -> {
					Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> keyValues = Utils.getStream(keys)
						.collect(Utils.toMapWithValues(c -> scalarFromColumnValue(keyRow.getAs(c.getName()), c)));
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
		throw new UnsupportedOperationException("VTL transformation not supported on Spark");
	}
	
	@Override
	public boolean isCacheable()
	{
		return false;
	}
}
