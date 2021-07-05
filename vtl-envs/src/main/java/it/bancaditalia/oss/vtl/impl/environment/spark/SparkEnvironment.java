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

import static it.bancaditalia.oss.vtl.impl.environment.spark.DataPointEncoder.getDataTypeForComponent;
import static it.bancaditalia.oss.vtl.impl.environment.util.CSVParseUtils.mapValue;
import static it.bancaditalia.oss.vtl.model.data.DataStructureComponent.byName;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.Utils.entriesToMap;
import static it.bancaditalia.oss.vtl.util.Utils.toMapWithValues;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.udf;

import java.io.Serializable;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.UDTRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;

import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.impl.environment.util.CSVParseUtils;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.impl.types.data.date.DayHolder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageCall;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageExternal;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageGroup;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageImpl;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.VTLValue;

public class SparkEnvironment implements Environment
{
	private static final Logger LOGGER = LoggerFactory.getLogger(SparkEnvironment.class);

	public static final VTLProperty VTL_SPARK_MASTER_CONNECTION = 
			new VTLPropertyImpl("vtl.spark.master.connection", "Connection string to an orchestrator or local", "local", true, false, "local");
	public static final VTLProperty VTL_SPARK_UI_ENABLED = 
			new VTLPropertyImpl("vtl.spark.ui.enabled", "Indicates if the Spark web UI should be initialized", "true", true, false, "true");
	public static final VTLProperty VTL_SPARK_PAGE_SIZE = 
			new VTLPropertyImpl("vtl.spark.page.size", "Indicates the buffer size when retrieving datapoints from Spark", "1000", true, false, "1000");

	static
	{
		ConfigurationManagerFactory.registerSupportedProperties(SparkEnvironment.class, VTL_SPARK_MASTER_CONNECTION, VTL_SPARK_PAGE_SIZE, VTL_SPARK_UI_ENABLED);
	}
	
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

	private final SparkSession session;
	private final Map<String, SparkDataSet> frames = new ConcurrentHashMap<>();
	private final DataFrameReader reader;

	public SparkEnvironment()
	{
		SparkConf conf = new SparkConf()
			  .setMaster(VTL_SPARK_MASTER_CONNECTION.getValue())
			  .setAppName("Spark SQL Environment for VTL Engine [" + hashCode() + "]")
			  .set("spark.executor.processTreeMetrics.enabled", "false")
			  .set("spark.kryo.registrator", "it.bancaditalia.oss.vtl.impl.environment.spark.SparkEnvironment$VTLKryoRegistrator")
			  .set("spark.sql.datetime.java8API.enabled", "true")
			  .set("spark.sql.shuffle.partitions", "4")
			  .set("spark.sql.catalyst.dateType", "Instant")
			  .set("spark.ui.enabled", Boolean.valueOf(VTL_SPARK_UI_ENABLED.getValue()).toString());
		
		session = SparkSession
			  .builder()
			  .config(conf)
			  .getOrCreate();
		
		reader = session.read();
		
		if (!UDTRegistration.exists(Lineage.class.getName()))
			UDTRegistration.register(Lineage.class.getName(), LineageSparkUDT.class.getName());
		if (!UDTRegistration.exists(DayHolder.class.getName()))
			UDTRegistration.register(DayHolder.class.getName(), DayHolderSparkUDT.class.getName());
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
		
		SparkDataSet dataset = null;
		switch (parts[0])
		{
			case "csv":
				dataset = inferSchema(reader.format("csv").option("header", "true").load(parts[1]), name); 
				break;
			case "text": case "parquet": case "json": 
				dataset = inferSchema(reader.format(parts[0]).load(parts[1]), name); 
				break;
			default: 
				throw new UnsupportedOperationException("Unsupported dataset format: " + parts[0]);
		}
		
		frames.put(name, dataset);
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
	
	private SparkDataSet inferSchema(Dataset<Row> sourceDataFrame, String alias)
	{
		// infer structure from header
		String[] fieldNames = sourceDataFrame.schema().fieldNames();
		Entry<List<DataStructureComponent<?, ?, ?>>, Map<DataStructureComponent<?, ?, ?>, String>> metaInfo = CSVParseUtils.extractMetadata(fieldNames);
		DataSetMetadata structure = new DataStructureBuilder(metaInfo.getKey()).build();
		
		// masks for decoding CSV rows
		Map<DataStructureComponent<?, ?, ?>, String> masks = metaInfo.getValue();
		
		// normalized column names in alphabetical order
		Map<String, String> newToOldNames = IntStream.range(0, fieldNames.length)
				.mapToObj(i -> new SimpleEntry<>(metaInfo.getKey().get(i).getName(), fieldNames[i]))
				.collect(entriesToMap());
		String[] normalizedNames = newToOldNames.keySet().toArray(new String[newToOldNames.size()]);
		Arrays.sort(normalizedNames, 0, normalizedNames.length);
		
		// array of parsers for CSV fields
		Map<DataStructureComponent<?, ?, ?>, DataType> types = structure.stream()
			.collect(toMapWithValues(c -> getDataTypeForComponent(c)));
		Column[] converters = Arrays.stream(normalizedNames, 0, normalizedNames.length)
				.map(structure::getComponent)
				.map(Optional::get)
				.sorted(byName())
				.map(c -> udf(valueMapper(c, masks.get(c), types.get(c)), types.get(c))
						.apply(sourceDataFrame.col(newToOldNames.get(c.getName())))
						.alias(c.getName()))
				.collect(toList())
				.toArray(new Column[normalizedNames.length + 1]);
		
		// add a column and a converter for the lineage
		byte[] serializedLineage = LineageSparkUDT$.MODULE$.serialize(LineageExternal.of("spark:" + alias));
		converters[converters.length - 1] = lit(serializedLineage).alias("$lineage$");
		
		return new SparkDataSet(session, structure, sourceDataFrame.select(converters));
	}
	
	private static UDF1<String, Serializable> valueMapper(DataStructureComponent<?, ?, ?> component, String mask, DataType type)
	{
		return new UDF1<String, Serializable>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Serializable call(String repr) throws Exception
			{
				LOGGER.trace("{}", type);
				Serializable value = (Serializable) mapValue(component, repr, mask).get();
				if (value instanceof DayHolder)
					value = ((DayHolder) value).getLocalDate();
				return value;
			}
		};
	}
}
