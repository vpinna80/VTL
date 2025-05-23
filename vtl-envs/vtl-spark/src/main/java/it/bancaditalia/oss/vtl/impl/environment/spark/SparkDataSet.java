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

import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkEnvironment.VTL_SPARK_PAGE_SIZE;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.getColumnsFromComponents;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.getDataTypeFor;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.getEncoderFor;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.getMetadataFor;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.getNamesFromComponents;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.getScalarFor;
import static it.bancaditalia.oss.vtl.impl.environment.spark.udts.LineageUDT.LineageSparkUDT;
import static it.bancaditalia.oss.vtl.model.transform.analytic.LimitCriterion.LimitDirection.PRECEDING;
import static it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion.SortingMethod.ASC;
import static it.bancaditalia.oss.vtl.model.transform.analytic.WindowCriterion.LimitType.RANGE;
import static it.bancaditalia.oss.vtl.util.SerCollectors.mapping;
import static it.bancaditalia.oss.vtl.util.SerCollectors.teeing;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toArray;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.SerUnaryOperator.identity;
import static it.bancaditalia.oss.vtl.util.Utils.SEQUENTIAL;
import static java.util.Arrays.binarySearch;
import static java.util.Collections.emptyMap;
import static org.apache.spark.sql.Encoders.INT;
import static org.apache.spark.sql.expressions.Window.partitionBy;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.udaf;
import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.types.DataTypes.createArrayType;
import static org.apache.spark.sql.types.DataTypes.createStructType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.execution.ExplainMode;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.Window$;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLInvariantIdentifiersException;
import it.bancaditalia.oss.vtl.impl.environment.spark.scalars.ScalarValueUDT;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.AbstractDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageExternal;
import it.bancaditalia.oss.vtl.impl.types.operators.PartitionToRank;
import it.bancaditalia.oss.vtl.impl.types.window.RankedPartition;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.transform.analytic.LimitCriterion;
import it.bancaditalia.oss.vtl.model.transform.analytic.SortCriterion;
import it.bancaditalia.oss.vtl.model.transform.analytic.WindowClause;
import it.bancaditalia.oss.vtl.util.SerBiConsumer;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerBiPredicate;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;
import it.bancaditalia.oss.vtl.util.SerCollector;
import it.bancaditalia.oss.vtl.util.SerFunction;
import it.bancaditalia.oss.vtl.util.SerPredicate;
import it.bancaditalia.oss.vtl.util.SerSupplier;
import it.bancaditalia.oss.vtl.util.SerTriFunction;
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;
import it.bancaditalia.oss.vtl.util.Utils;

public class SparkDataSet extends AbstractDataSet
{
	private static final Logger LOGGER = LoggerFactory.getLogger(SparkDataSet.class);
	private static final Lineage IGNORED_LINEAGE = LineageExternal.of("ignored");
	private static final ReferenceQueue<SparkDataSet> REF_QUEUE = new ReferenceQueue<>();
	private static final Map<WeakReference<SparkDataSet>, Dataset<Row>> SPARK_CACHE = new ConcurrentHashMap<>();

	static {
		new Thread() {
			
			{
				setName("SparkDataSet gc watcher");
				setDaemon(true);
			}
			
			@Override
			public void run() 
			{
				while (!Thread.interrupted())
					try 
					{
						Reference<? extends SparkDataSet> ref;
						while ((ref = REF_QUEUE.poll()) != null)
						{
							Dataset<Row> data = SPARK_CACHE.remove(ref);
							if (data != null)
								data.unpersist();
						}
						
						Thread.sleep(5000);
					}
					catch (InterruptedException e)
					{
						Thread.currentThread().interrupt();
					}
			}
			
		}.start();
	}
	
	private final SparkSession session;
	private final DataPointEncoder encoder;
	private final Dataset<Row> dataFrame;
	private transient final WeakReference<SparkDataSet> ref = new WeakReference<SparkDataSet>(this, REF_QUEUE);
	
	public SparkDataSet(SparkSession session, DataSetMetadata structure, DataPointEncoder encoder, Dataset<Row> dataFrame)
	{
		super(structure);
		
		for (DataStructureComponent<?, ?, ?> component: encoder.getComponents())
		{
			VTLAlias name = component.getVariable().getAlias();
			dataFrame = dataFrame.withColumn(name.getName(), dataFrame.col(name.getName()), getMetadataFor(component));
		}
		dataFrame = dataFrame.withColumn("$lineage$", dataFrame.col("$lineage$"), Metadata.empty());
		
		this.session = session;
		this.encoder = encoder;
		this.dataFrame = dataFrame.cache();

		SPARK_CACHE.put(ref, this.dataFrame);
		logInfo(toString());
	}

	public SparkDataSet(SparkSession session, DataSetMetadata structure, Dataset<Row> dataFrame)
	{
		this(session, structure, new DataPointEncoder(session, structure), dataFrame);

		logInfo(toString());
	}

	public SparkDataSet(SparkSession session, DataSetMetadata dataStructure, DataSet toWrap)
	{
		this(session, dataStructure, loadIntoSpark(session, toWrap).cache());

		logInfo(toString());
	}

	private static Dataset<Row> loadIntoSpark(SparkSession session, DataSet toWrap)
	{
		DataSetMetadata dataStructure = toWrap.getMetadata();
		LOGGER.info("Loading data into Spark dataframe...", dataStructure);
		
		try (Stream<DataPoint> stream = toWrap.stream())
		{
			DataPointEncoder encoder = new DataPointEncoder(session, toWrap.getMetadata());
			List<Row> rows = stream.map(encoder::encode).collect(toList());
			Encoder<Row> rowEncoder = new DataPointEncoder(session, toWrap.getMetadata()).getRowEncoder();
			return session.createDataset(rows, rowEncoder);
		}
	}
	
	@Override
	protected Stream<DataPoint> streamDataPoints()
	{
		int bufferSize = Integer.parseInt(VTL_SPARK_PAGE_SIZE.getValue());
		LOGGER.warn("Streaming datapoints into the driver from Spark dataset {}", getMetadata());

		return StreamSupport.stream(new SparkSpliterator(dataFrame.toLocalIterator(), bufferSize), !SEQUENTIAL).map(encoder::decode);
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
	public DataSet membership(VTLAlias alias, SerUnaryOperator<Lineage> lineageOp)
	{
		DataSetMetadata membershipStructure = getMetadata().membership(alias);
		LOGGER.debug("Creating dataset by membership on {} from {} to {}", alias, getMetadata(), membershipStructure);
		
		Column[] cols = getColumnsFromComponents(membershipStructure).toArray(Column[]::new);
		Dataset<Row> newDF = getMetadata().getComponent(alias)
			.filter(comp -> !comp.is(Measure.class))
			.map(comp -> {
				VTLAlias defaultName = comp.getVariable().getDomain().getDefaultVariable().getAlias();
				return dataFrame.withColumn(defaultName.getName(), dataFrame.col(alias.getName()));
			}).orElse(dataFrame)
			.select(cols);
		
		Row lineageRow = new GenericRow(new Object[] { LineageExternal.of("#" + alias) });
		Dataset<Row> lineageDF = session.createDataFrame(List.of(lineageRow), createStructType(List.of(new StructField("$lineage$", LineageSparkUDT, false, Metadata.empty()))));
		return new SparkDataSet(session, membershipStructure, newDF.drop("$lineage$").crossJoin(lineageDF));
	}

	@Override
	public DataSet filter(SerPredicate<DataPoint> predicate, SerUnaryOperator<Lineage> lineageOperator)
	{
		DataPointEncoder serEncoder = encoder;
		FilterFunction<Row> func = row -> predicate.test(serEncoder.decode(row));
		Column lineageCol = dataFrame.col("$lineage$");
		return new SparkDataSet(session, getMetadata(), dataFrame.filter(func)
				.withColumn("$lineage$", udf((UDF1<Lineage, Lineage>) lineageOperator::apply, LineageSparkUDT).apply(lineageCol)));
	}
	
	@Override
	public DataSet getMatching(Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> keyValues)
	{
		return filter(dp -> keyValues.equals(dp.getValues(keyValues.keySet(), Identifier.class)), identity());	
	}
	
	@Override
	public long size()
	{
		return dataFrame.count();
	}

	@Override
	public DataSet subspace(Map<? extends DataStructureComponent<? extends Identifier, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> keyValues, SerUnaryOperator<Lineage> lineageOperator)
	{
		DataSetMetadata newMetadata = new DataStructureBuilder(getMetadata()).removeComponents(keyValues.keySet()).build();
		DataPointEncoder newEncoder = new DataPointEncoder(session, newMetadata);
		
		Dataset<Row> subbed = dataFrame;
		for (Entry<? extends DataStructureComponent<? extends Identifier, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> e: keyValues.entrySet())
		{
			String name = e.getKey().getVariable().getAlias().getName();
			ScalarValueUDT<?> dataType = (ScalarValueUDT<?>) getDataTypeFor(e.getKey());
			subbed = subbed.withColumn(name + "$$SUB$$", new Column(Literal.create(dataType.serializeUnchecked(e.getValue()), dataType)))
					.where(col(name).equalTo(col(name + "$$SUB$$")))
					.drop(name, name + "$$SUB$$");
		}
		
		subbed = subbed.withColumn("$lineage$", udf((UDF1<Lineage, Lineage>) lineageOperator::apply, LineageSparkUDT).apply(col("$lineage$")));
		
		return new SparkDataSet(session, newMetadata, newEncoder, subbed);
	}
	
	@Override
	public DataSet mapKeepingKeys(DataSetMetadata metadata, SerUnaryOperator<Lineage> lineageOperator,
			SerFunction<? super DataPoint, ? extends Map<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>>> operator)
	{
		Set<DataStructureComponent<Identifier, ?, ?>> originalIDs = getMetadata().getIDs();
		if (!metadata.getIDs().containsAll(originalIDs))
			throw new VTLInvariantIdentifiersException("map", originalIDs, metadata.getIDs());
		
		LOGGER.trace("Creating dataset by mapping from {} to {}", getMetadata(), metadata);
		
		DataPointEncoder resultEncoder = new DataPointEncoder(session, metadata);
		DataPointEncoder serEncoder = encoder;
		DataStructureComponent<?, ?, ?>[] destComps = resultEncoder.getComponents();
		int[] idIndex = new int[originalIDs.size()];
		for (int i = 0; i < idIndex.length; i++)
			idIndex[i] = binarySearch(resultEncoder.getComponents(), serEncoder.getComponents()[i], DataStructureComponent::byNameAndRole);
		
		Dataset<Row> result = dataFrame.map((MapFunction<Row, Row>) row -> {
			Serializable[] newRow = new Serializable[metadata.size() + 1];
			for (int i = 0; i < idIndex.length; i++)
				newRow[idIndex[i]] = row.getAs(i);
			
			DataPoint original = serEncoder.decode(row);
			Map<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> newComps = operator.apply(original);
			for (int i = 0; i < destComps.length; i++)
				if (newRow[i] == null)
				{
					ScalarValue<?, ?, ?, ?> newVal = newComps.get(destComps[i]);
					if (newVal != null &!newVal.isNull())
						newRow[i] = newVal;
				}
			
			newRow[newRow.length - 1] = lineageOperator.apply(original.getLineage());
			return new GenericRow(newRow);
		}, resultEncoder.getRowEncoder());

		return new SparkDataSet(session, metadata, resultEncoder, result);
	}
	
	@Override
	public DataSet flatmapKeepingKeys(DataSetMetadata metadata, SerUnaryOperator<Lineage> lineageOperator,
			SerFunction<? super DataPoint, ? extends Stream<? extends Map<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>>>> operator)
	{
		DataPointEncoder resultEncoder = new DataPointEncoder(session, metadata);
		DataStructureComponent<?, ?, ?>[] comps = resultEncoder.getComponents();
		Set<DataStructureComponent<Identifier, ?, ?>> ids = getMetadata().getIDs();
		StructType schema = resultEncoder.getSchema();
		
		DataPointEncoder serEncoder = encoder;
		FlatMapFunction<Row, Row> flatMapper = row -> {
			SparkDataPoint dp = serEncoder.decode(row);
			Serializable[] vals = new Serializable[comps.length + 1];
			for (int i = 0; i < comps.length && comps[i].is(Identifier.class); i++)
				vals[i] = row.getAs(i);
			vals[vals.length - 1] = lineageOperator.apply(dp.getLineage());
			
			return operator.apply(dp)
				.map(map -> {
					Serializable[] finalVals = Arrays.copyOf(vals, vals.length);
					for (int i = 0; i < comps.length; i++)
					{
						// fill a copy of the template 
						DataStructureComponent<?, ?, ?> c = comps[i];
						if (!ids.contains(c))
						{
							ScalarValue<?, ?, ?, ?> value = map.get(c);
							finalVals[i] = value.isNull() ? null : value;
						}
					}
					return (Row) new GenericRowWithSchema(finalVals, schema);
				}).iterator();
		};
		Dataset<Row> flattenedDf = dataFrame.flatMap(flatMapper, resultEncoder.getRowEncoder());
		
		return new SparkDataSet(session, metadata, resultEncoder, flattenedDf);
	}
	
	@Override
	public DataSet filteredMappedJoin(DataSetMetadata metadata, DataSet other, SerBiPredicate<DataPoint, DataPoint> where, SerBinaryOperator<DataPoint> mergeOp, boolean leftJoin)
	{
		SparkDataSet sparkOther = other instanceof SparkDataSet ? ((SparkDataSet) other) : new SparkDataSet(session, other.getMetadata(), other);

		List<String> commonIDs = getMetadata().getIDs().stream()
				.filter(other.getMetadata()::contains)
				.map(c -> c.getVariable().getAlias().getName())
				.collect(toList());

		Dataset<Row> leftDataframe = dataFrame.as("a");
		Dataset<Row> rightDataframe = sparkOther.dataFrame.as("b");
		int leftSize = getMetadata().size();
		
		// TODO: Try to execute filter with a null datapoint?
		DataPointEncoder serEncoder = encoder;
		DataPointEncoder otherEncoder = sparkOther.encoder;
		
		Column joinKeys = null;
		for (String commonID: commonIDs)
		{
			Column newCond = leftDataframe.col(commonID).equalTo(rightDataframe.col(commonID)); 
			joinKeys = joinKeys == null ? newCond : joinKeys.and(newCond);
		}

		Dataset<Row> joined; 
		if (commonIDs.isEmpty())
			joined = leftDataframe.crossJoin(rightDataframe);
		else
			joined = leftDataframe.join(rightDataframe, joinKeys, leftJoin ? "left" : "inner");
		
		if (where != DataSet.ALL)
			joined = joined.filter((FilterFunction<Row>) row -> {
				DataPoint right = leftJoin && row.get(leftSize + 1) == null ? null : otherEncoder.decode(row, leftSize + 1);
				return where.test(serEncoder.decode(row), right);
			});
		
		DataPointEncoder resultEncoder = new DataPointEncoder(session, metadata);
		MapFunction<Row, Row> sparkMerge = row -> {
			DataPoint right = leftJoin && row.get(leftSize + 1) == null ? null : otherEncoder.decode(row, leftSize + 1);
			return resultEncoder.encode(mergeOp.apply(serEncoder.decode(row), right));
		};
		joined = joined.map(sparkMerge, resultEncoder.getRowEncoder());
		return new SparkDataSet(session, metadata, joined);
	}
	
	@Override
	public <T, TT> DataSet analytic(SerUnaryOperator<Lineage> lineageOp, DataStructureComponent<?, ?, ?> sourceComp,
			DataStructureComponent<?, ?, ?> destComp, WindowClause clause, SerFunction<DataPoint, T> extractor, 
			SerCollector<T, ?, TT> collector, SerBiFunction<TT, T, Collection<? extends ScalarValue<?, ?, ?, ?>>> finisher)
	{
		// Convert a VTL window clause to a Spark Window Specification
		WindowSpec windowSpec = Window$.MODULE$.spec();
		if (!clause.getPartitioningComponents().isEmpty())
			windowSpec = Window.partitionBy(clause.getPartitioningComponents().stream()
					.map(c -> c.getVariable().getAlias().getName())
					.map(dataFrame::col)
					.collect(toArray(new Column[clause.getPartitioningComponents().size()])));

		Column[] orderBy = new Column[clause.getSortCriteria().size()];
		for (int i = 0; i < orderBy.length; i++)
		{
			SortCriterion item = clause.getSortCriteria().get(i);
			Column col = dataFrame.col(item.getComponent().getVariable().getAlias().getName());
			orderBy[i] = item.getMethod() == ASC ? col.asc() : col.desc();
		}
		
		if (orderBy.length > 0)
			windowSpec = windowSpec.orderBy(orderBy);
		
		LimitCriterion infBound = clause.getWindowCriterion().getInfBound();
		LimitCriterion supBound = clause.getWindowCriterion().getSupBound();
		
		long inf = infBound.getCount();  
		long sup = supBound.getCount();
		if (infBound.getDirection() == PRECEDING)
			inf = inf == Long.MAX_VALUE ? Long.MIN_VALUE : -inf;
		if (supBound.getDirection() == PRECEDING)
			sup = sup == Long.MAX_VALUE ? Long.MIN_VALUE : -sup;
			
		windowSpec = clause.getWindowCriterion().getType() == RANGE 
				? windowSpec.rangeBetween(inf, sup)
				: windowSpec.rowsBetween(inf, sup);

		// Infer the accumulator and result types to create the relative encoder
		DataSetMetadata structure = getMetadata();
		Serializable accum = (Serializable) collector.supplier().get();
		Encoder<?> accEncoder = ExpressionEncoder.apply(getEncoderFor(accum, structure));
		@SuppressWarnings("unchecked")
		Serializable tag = ((SerFunction<Object, Serializable>) collector.finisher()).apply(accum);
		ScalarValueUDT<?> tttype = (ScalarValueUDT<?>) getDataTypeFor(destComp);
		Encoder<TT> ttEncoder = ExpressionEncoder.apply(getEncoderFor(tag, structure));
		
		@SuppressWarnings("unchecked")
		Encoder<T> inputEncoder = (Encoder<T>) (extractor == null 
				? ExpressionEncoder.apply(getEncoderFor(NullValue.instanceFrom(sourceComp), null)) 
				: encoder.getRowEncoder());
		
		DataPointEncoder serEncoder = encoder;
		Column[] allColumns = Arrays.stream(dataFrame.columns()).map(functions::col).toArray(Column[]::new);
		Column[] columns = extractor != null ? allColumns : new Column[] { col(sourceComp.getVariable().getAlias().getName()) };
		
		// patch the collector's finisher so that GenericRows in PartitionToRank are correctly decoded to data points
		if (tag instanceof RankedPartition)
			collector = patchCollector(collector, serEncoder);
		
		String destName = destComp.getVariable().getAlias().getName();
		VTLSparkAggregator<T, ?, TT, ScalarValue<?, ?, ?, ?>> vtlAggr = new VTLSparkAggregator<>(collector, accEncoder, ttEncoder);
		Dataset<Row> analyticResult = dataFrame.withColumn("$$window$$", udaf(vtlAggr, inputEncoder).apply(columns).over(windowSpec));

		if (finisher != null)
		{
			// Check if the extractor is non null, in which case it may be a signal that the finisher takes 
			// the original DataPoint as the second parameter (this is best-effort, it may be the case that 
			// the second parameter is neither a ScalarValue nor a DataPoint).
			@SuppressWarnings("unchecked")
			UDF2<TT, Row, Collection<? extends ScalarValue<?, ?, ?, ?>>> finisherUDF = (toFinish, t) -> {
				return finisher.apply(toFinish, (T) (extractor != null ? serEncoder.decode(t) : (ScalarValue<?, ?, ?, ?>) t.get(0)));
			};
			
			analyticResult = analyticResult.withColumn("$$window$$", explode(udf(finisherUDF, createArrayType(tttype))
					.apply(col("$$window$$"), struct(columns))));
		}
		analyticResult = analyticResult.withColumn(destName, col("$$window$$")).drop(col("$$window$$"));
		
		// Structure of the result
		DataSetMetadata newStructure = sourceComp.equals(destComp) ? structure : new DataStructureBuilder(getMetadata())
				.addComponent(destComp)
				.build();
		
		DataPointEncoder resultEncoder = new DataPointEncoder(session, newStructure);
		Column[] cols = getColumnsFromComponents(newStructure).toArray(new Column[newStructure.size() + 1]);
		cols[cols.length - 1] = analyticResult.col("$lineage$");
		
		return new SparkDataSet(session, newStructure, resultEncoder, analyticResult.select(cols));
	}

	@SuppressWarnings("unchecked")
	private <T, TT> SerCollector<T, ?, TT> patchCollector(SerCollector<T, ?, TT> collector, DataPointEncoder serEncoder)
	{
		// UGLY; it has to do with the PartitionToRankUDT and the fact that GenericRows are not deserialized by spark.
		return innerPatchCollector((SerCollector<T, PartitionToRank, TT>) collector, serEncoder);
	}
	
	private <T, TT> SerCollector<T, ?, TT> innerPatchCollector(SerCollector<T, PartitionToRank, TT> collector, DataPointEncoder serEncoder)
	{
		SerSupplier<PartitionToRank> supplier = (SerSupplier<PartitionToRank>) collector.supplier();
		SerBiConsumer<PartitionToRank, T> accumulator = (SerBiConsumer<PartitionToRank, T>) collector.accumulator();
		SerBinaryOperator<PartitionToRank> combiner = (SerBinaryOperator<PartitionToRank>) collector.combiner();
		SerFunction<PartitionToRank, TT> patchedFinisher = ((SerUnaryOperator<PartitionToRank>) ptr -> {
			ListIterator<DataPoint> iter = ptr.listIterator();
			while (iter.hasNext())
				iter.set(serEncoder.decode((Row) iter.next()));
			return ptr;
		}).andThen((SerFunction<PartitionToRank, TT>) collector.finisher());
		return SerCollector.of(supplier, accumulator, combiner, patchedFinisher, collector.characteristics());
	}

	@Override
	public <T extends Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>, TT> VTLValue aggregate(VTLValueMetadata metadata,
			Set<DataStructureComponent<Identifier, ?, ?>> keys, SerCollector<DataPoint, ?, T> groupCollector,
			SerTriFunction<? super T, ? super List<Lineage>, ? super Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, TT> finisher)
	{
		DataSetMetadata structure;
		if (metadata.isDataSet())
			structure = (DataSetMetadata) metadata;
		else
			structure = new DataStructureBuilder(((ScalarValueMetadata<?, ?>) metadata).getDomain().getDefaultVariable().as(Measure.class)).build();
		DataPointEncoder resultEncoder = new DataPointEncoder(session, structure);
		int bufferSize = Integer.parseInt(VTL_SPARK_PAGE_SIZE.getValue());
		Dataset<Row> aggred;
		
		DataPointEncoder serEncoder = encoder;
		if (keys.isEmpty())
		{
			MapGroupsFunction<Integer, Row, Row> aggregator = (i, s) -> 
				StreamSupport.stream(new SparkSpliterator(s, bufferSize), !SEQUENTIAL)
					.map(serEncoder::decode)
					.collect(teeing(mapping(DataPoint::getLineage, toList()), groupCollector, (l, aggr) -> {
						TT finished = finisher.apply(aggr, l, emptyMap());
						if (finished instanceof DataPoint)
							return resultEncoder.encode((DataPoint) finished);
						
						ScalarValue<?, ?, ?, ?>[] result = new ScalarValue<?, ?, ?, ?>[] { (ScalarValue<?, ?, ?, ?>) finished };
						return resultEncoder.encode(new SparkDataPoint(resultEncoder.getComponents(), result, IGNORED_LINEAGE));
					}));
			
			aggred = dataFrame.groupBy(lit(1))
					.as(INT(), serEncoder.getRowEncoder())
					.mapGroups(aggregator, resultEncoder.getRowEncoder());
		}
		else
		{
			DataPointEncoder keyEncoder = new DataPointEncoder(session, keys);
			Column[] keyNames = new Column[keys.size()];
			for (int i = 0; i < keyNames.length; i++)
				keyNames[i] = dataFrame.col(keyEncoder.getComponents()[i].getVariable().getAlias().getName());
			
			MapGroupsFunction<Row, Row, Row> aggregator = (keyRow, s) -> {
					Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> keyValues = new HashMap<>();
					for (DataStructureComponent<Identifier, ?, ?> key: keys)
					{
						Variable<?, ?> v = key.getVariable();
						keyValues.put(key, getScalarFor(v.getDomain(), keyRow.getAs(v.getAlias().getName())));
					}
					
					return StreamSupport.stream(new SparkSpliterator(s, bufferSize), !Utils.SEQUENTIAL)
							.map(serEncoder::decode)
							.collect(teeing(mapping(DataPoint::getLineage, toList()), groupCollector, (l, aggr) -> 
								resultEncoder.encode((DataPoint) finisher.apply(aggr, l, keyValues))));
				};
			
			aggred = dataFrame.groupBy(keyNames)
					.as(keyEncoder.getRowEncoderNoLineage(), serEncoder.getRowEncoder())
					.mapGroups(aggregator, resultEncoder.getRowEncoder());
		}
		
		if (metadata.isDataSet())
			return new SparkDataSet(session, structure, aggred);
		else
		{
			List<Row> results = aggred.collectAsList();
			if (results.size() != 1)
				throw new IllegalStateException("Expected a single datapoint but found more than one.");
			
			DataPoint dp = new DataPointEncoder(session, structure).decode(results.get(0));
			if (dp.size() != 1)
				throw new IllegalStateException("Expected a single measure but found a multicomponent dataset.");
			
			return dp.values().iterator().next();
		}
	}

	@Override
	public DataSet union(List<DataSet> others, SerUnaryOperator<Lineage> lineageOp)
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
		
		Row lineageRow = new GenericRow(new Object[] { LineageExternal.of("Union") });
		
		StructType lineageType = createStructType(List.of(new StructField("$lineage$", LineageSparkUDT, false, Metadata.empty())));
		result = result.withColumn("__index", row_number().over(partitionBy(ids).orderBy(col("__index"))))
				.filter(col("__index").equalTo(1))
				.drop("__index")
				.select(cols)
				.join(session.createDataFrame(List.of(lineageRow), lineageType));
		
		return new SparkDataSet(session, getMetadata(), encoder, result);
	}

	private void logInfo(String description)
	{
		LOGGER.info("Spark execution plan prepared for {}", description);
		LOGGER.trace("{}", dataFrame.queryExecution().explainString(ExplainMode.fromString("formatted")));
	}

	private static void checkSerializable(Object obj)
	{
	    try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutputStream out = new ObjectOutputStream(bos))
	    {
	        out.writeObject(obj);
	    }
	    catch (IOException e)
	    {
	        throw new UncheckedIOException("Not serializable: " + obj.getClass().getName(), e);
	    }
	}
}
