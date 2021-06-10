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

import static it.bancaditalia.oss.vtl.impl.environment.util.CSVParseUtils.mapValue;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.toDataPoint;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder.toDataStructure;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NULLDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.util.Utils.entriesToMap;
import static it.bancaditalia.oss.vtl.util.Utils.keepingKey;
import static it.bancaditalia.oss.vtl.util.Utils.keepingValue;
import static it.bancaditalia.oss.vtl.util.Utils.toEntry;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithKey;
import static scala.collection.JavaConverters.asScalaSet;
import static scala.collection.JavaConverters.mapAsJavaMap;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;

import com.esotericsoftware.kryo.Kryo;

import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.impl.environment.util.CSVParseUtils;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageCall;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageExternal;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageGroup;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageImpl;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageSet;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.util.Utils;
import scala.collection.Seq;

public class SparkEnvironment implements Environment
{
	private final SparkSession session;
	private final Map<String, SparkDataSet> frames = new ConcurrentHashMap<>();
	private final DataFrameReader reader;

	public static final VTLProperty VTL_SPARK_MASTER_CONNECTION = 
			new VTLPropertyImpl("vtl.spark.master.connection", "Connection string to an orchestrator or local", "local", true, false, "local");
	public static final VTLProperty VTL_SPARK_UI_ENABLED = 
			new VTLPropertyImpl("vtl.spark.ui.enabled", "Indicates if the Spark web UI should be initialized", "local", true, false, "true");
	
	public static class VTLKryoRegistrator implements KryoRegistrator
	{
		@Override
		public void registerClasses(Kryo kryo)
		{
			LineageSerializer lineageSerializer = new LineageSerializer();
			kryo.register(LineageExternal.class, lineageSerializer);
			kryo.register(LineageGroup.class, lineageSerializer);
			kryo.register(LineageCall.class, lineageSerializer);
			kryo.register(LineageNode.class, lineageSerializer);
			kryo.register(LineageImpl.class, lineageSerializer);
			kryo.register(LineageSet.class, lineageSerializer);
			kryo.register(Lineage.class, lineageSerializer);
		}
	}

	public SparkEnvironment()
	{
		SparkConf conf = new SparkConf()
			  .setMaster(VTL_SPARK_MASTER_CONNECTION.getValue())
			  .setAppName("Spark SQL Environment for VTL Engine [" + hashCode() + "]")
			  .set("spark.executor.processTreeMetrics.enabled", "false")
			  .set("spark.executor.uri", "file:///home/user/public/m027907/spark-3.1.2-bin-hadoop3.2.tgz")
			  .set("spark.driver.host", "stu068.utenze.bankit.it")
			  .set("spark.kryo.registrator", "it.bancaditalia.oss.vtl.impl.environment.spark.SparkEnvironment$VTLKryoRegistrator")
			  .set("spark.ui.enabled", Boolean.valueOf(VTL_SPARK_UI_ENABLED.getValue()).toString());
		
		session = SparkSession
			  .builder()
			  .config(conf)
			  .getOrCreate();
		
		reader = session.read();
	}
	
	@Override
	public boolean contains(String name)
	{
		name = name.matches("'.*'") ? name.replaceAll("'(.*)'", "$1") : name.toLowerCase();
		if (!name.startsWith("spark:"))
			return false;
		name = name.substring(6);
		
		if (frames.containsKey(name))
			return true;
		
		String[] parts = name.split(":", 2);
		if (parts.length != 2)
			return false;
		
		SparkDataSet df = null;
		switch (parts[0])
		{
			case "csv":
				df = inferSchema(reader.format("csv").option("header", "true").load(parts[1]), name); 
				break;
			case "text": case "parquet": case "json": 
				df = inferSchema(reader.format(parts[0]).load(parts[1]), name); 
				break;
			default: 
				throw new UnsupportedOperationException("Unsupported dataset format: " + parts[0]);
		}
		
		frames.put(name, df);
		return true;
	}

	@Override
	public Optional<VTLValue> getValue(String name)
	{
		if (!contains(name))
			return Optional.empty();

		String normalizedName = (name.matches("'.*'") ? name.replaceAll("'(.*)'", "$1") : name.toLowerCase()).substring(6);
		return Optional.of(frames.get(normalizedName));
	}

	private static DataStructureComponent<?, ?, ?> componentFromField(StructField field)
	{
		String name = field.name();
		
		ValueDomainSubset<?, ?> domain;
		DataType fieldSparkType = field.dataType();
		if (fieldSparkType instanceof ByteType || fieldSparkType instanceof ShortType || fieldSparkType instanceof IntegerType || fieldSparkType instanceof LongType)
			domain = INTEGERDS;
		else if (fieldSparkType instanceof FloatType || fieldSparkType instanceof DoubleType)
			domain = NUMBERDS;
		else if (fieldSparkType instanceof StringType)
			domain = STRINGDS;
		else if (fieldSparkType instanceof NullType)
			domain = NULLDS;
		else if (fieldSparkType instanceof DateType)
			domain = DATEDS;
		else
			throw new UnsupportedOperationException("Unsupported dataframe field type: " + fieldSparkType);

		Class<? extends ComponentRole> role = domain == NUMBERDS ? Measure.class : Identifier.class;

		return DataStructureComponentImpl.of(name, role, domain);
	}
	
	private SparkDataSet inferSchema(Dataset<Row> sourceDataFrame, String alias)
	{
		DataStructureBuilder builder = new DataStructureBuilder();
		StructField[] fields = sourceDataFrame.schema().fields();
		MapFunction<Row, DataPoint> extractor;
		
		DataSetMetadata structure;
		if (Arrays.stream(fields).anyMatch(f -> f.name().matches("^[$#]?.+=[^\\[]+(\\[.*\\])?$")))
		{
			/* Special structure inferring from VTL-CSV header */ 
			String[] fieldNames = sourceDataFrame.schema().fieldNames();
			Entry<List<DataStructureComponent<?, ?, ?>>, Map<DataStructureComponent<?, ?, ?>, String>> metaInfo = CSVParseUtils.extractMetadata(fieldNames);
			structure = metaInfo.getKey().stream().collect(toDataStructure());
			for (int i = 0; i < fieldNames.length; i++)
			{
				String originalName = fieldNames[i];
				String normalizedName = metaInfo.getKey().get(i).getName();
				sourceDataFrame = sourceDataFrame.withColumnRenamed(originalName, normalizedName);
			}
			extractor = dpCSVExtractor(structure, alias, metaInfo.getValue());
		}
		else
		{
			for (StructField field: fields)
				builder.addComponent(componentFromField(field));
			structure = builder.build();
			extractor = dpExtractor(structure, alias);
		}
		
		DataPointEncoder encoder = new DataPointEncoder(structure);
		Dataset<Row> parsedDataFrame = sourceDataFrame.map(encoder.wrapWithEncoder(extractor), encoder.getRowEncoder());
		return new SparkDataSet(session, structure, parsedDataFrame);
	}

	private static MapFunction<Row, DataPoint> dpExtractor(DataSetMetadata structure, String alias)
	{
		Map<String, DataStructureComponent<?, ?, ?>> names = Utils.getStream(structure)
				.map(toEntryWithKey(DataStructureComponent::getName))
				.collect(entriesToMap());	
		Seq<String> scalaNames = asScalaSet(names.keySet()).toSeq();
		
		return row -> Utils.getStream(mapAsJavaMap(row.getValuesMap(scalaNames)))
			.map(toEntry(e -> names.get(e.getKey()), e -> sparkValueToScalar(names.get(e.getKey()), e.getValue())))
			.collect(toDataPoint(LineageExternal.of("spark:" + alias), structure, SparkDataPoint::new));
	}

	private static MapFunction<Row, DataPoint> dpCSVExtractor(DataSetMetadata structure, String alias, Map<DataStructureComponent<?, ?, ?>, String> masks)
	{
		Map<String, DataStructureComponent<?, ?, ?>> names = Utils.getStream(structure)
				.map(toEntryWithKey(DataStructureComponent::getName))
				.collect(entriesToMap());	
		Seq<String> scalaNames = asScalaSet(names.keySet()).toSeq();
		
		return row -> Utils.getStream(mapAsJavaMap(row.getValuesMap(scalaNames)))
			.map(keepingValue(names::get))
			.map(keepingKey((k, v) -> mapValue(k, (String) v, masks.get(k))))
			.collect(toDataPoint(LineageExternal.of("spark:" + alias), structure, SparkDataPoint::new));
	}

	private static ScalarValue<?, ?, ?, ?> sparkValueToScalar(DataStructureComponent<?, ?, ?> component, Object value)
	{
		if (value == null)
			return NullValue.instanceFrom(component);
		else if (value instanceof scala.Byte)
			return IntegerValue.of(((scala.Byte) value).toLong());
		else if (value instanceof scala.Short)
			return IntegerValue.of(((scala.Short) value).toLong());
		else if (value instanceof scala.Int)
			return IntegerValue.of(((scala.Int) value).toLong());
		else if (value instanceof scala.Long)
			return IntegerValue.of(((scala.Long) value).toLong());
		else if (value instanceof String)
			return StringValue.of((String) value);
		else if (value instanceof scala.Float)
			return DoubleValue.of(((scala.Float) value).toDouble());
		else if (value instanceof scala.Double)
			return DoubleValue.of(((scala.Double) value).toDouble());
		else if (value instanceof scala.Int)
			return IntegerValue.of(((scala.Int) value).toLong());
		else 
			throw new UnsupportedOperationException("Spark type not supported: " + value.getClass());
	}
}
