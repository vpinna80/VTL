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

import static it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory.registerSupportedProperties;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.getColumnsFromComponents;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.getComponentsFromStruct;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.getDataTypeFor;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.getMetadataFor;
import static it.bancaditalia.oss.vtl.impl.environment.util.CSVParseUtils.extractMetadata;
import static it.bancaditalia.oss.vtl.impl.environment.util.CSVParseUtils.mapValue;
import static it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl.Flags.REQUIRED;
import static it.bancaditalia.oss.vtl.util.SerCollectors.entriesToMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithValues;
import static org.apache.spark.sql.SaveMode.Overwrite;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.types.DataTypes.LongType;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.UDTRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;

import it.bancaditalia.oss.vtl.config.VTLProperty;
import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.impl.types.config.VTLPropertyImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageCall;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageExternal;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageImpl;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageSet;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class SparkEnvironment implements Environment
{
	private static final Logger LOGGER = LoggerFactory.getLogger(SparkEnvironment.class);

	public static final VTLProperty VTL_SPARK_MASTER_CONNECTION = 
			new VTLPropertyImpl("vtl.spark.master.connection", "Connection string to an orchestrator or local", "local[*]", EnumSet.of(REQUIRED), "local[*]");
	public static final VTLProperty VTL_SPARK_UI_ENABLED = 
			new VTLPropertyImpl("vtl.spark.ui.enabled", "Indicates if the Spark web UI should be initialized", "true", EnumSet.of(REQUIRED), "true");
	public static final VTLProperty VTL_SPARK_UI_PORT = 
			new VTLPropertyImpl("vtl.spark.ui.port", "Indicates which port the Spark web UI should be listening to", "4040", EnumSet.of(REQUIRED), "4040");
	public static final VTLProperty VTL_SPARK_PAGE_SIZE = 
			new VTLPropertyImpl("vtl.spark.page.size", "Indicates the buffer size when retrieving datapoints from Spark", "1000", EnumSet.of(REQUIRED), "1000");

	/* package */ static final LineageSparkUDT LineageSparkUDT = new LineageSparkUDT();
	
	static
	{
		registerSupportedProperties(SparkEnvironment.class, VTL_SPARK_MASTER_CONNECTION, VTL_SPARK_UI_ENABLED, VTL_SPARK_UI_PORT, VTL_SPARK_PAGE_SIZE);
		if (!UDTRegistration.exists(Lineage.class.getName()))
		{
			List<Class<? extends Lineage>> lClasses = List.of(LineageExternal.class, LineageCall.class, LineageNode.class, LineageImpl.class, LineageSet.class, Lineage.class);
			for (Class<?> lineageClass: lClasses)
				UDTRegistration.register(lineageClass.getName(), LineageSparkUDT.class.getName());
		}
	}
	
	public static class VTLKryoRegistrator implements KryoRegistrator
	{
		@Override
		public void registerClasses(Kryo kryo)
		{
			LineageSerializer lineageSerializer = new LineageSerializer();
			kryo.register(LineageExternal.class, lineageSerializer);
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
		String master = VTL_SPARK_MASTER_CONNECTION.getValue();
		LOGGER.info("Connecting to Spark master {}", master);
		SparkConf conf = new SparkConf()
			  .setMaster(master)
			  .setAppName("Spark SQL Environment for VTL Engine [" + hashCode() + "]")
			  .set("spark.executor.processTreeMetrics.enabled", "false")
			  .set("spark.kryo.registrator", "it.bancaditalia.oss.vtl.impl.environment.spark.SparkEnvironment$VTLKryoRegistrator")
			  .set("spark.sql.datetime.java8API.enabled", "true")
			  .set("spark.sql.catalyst.dateType", "Instant")
			  .set("spark.executor.instances", "4")
			  .set("spark.executor.cores", "2")
//			  .set("spark.sql.codegen.wholeStage", "false")
			  .set("spark.sql.windowExec.buffer.in.memory.threshold", "16384")
			  .set("spark.sql.caseSensitive", "true")
			  .set("spark.executor.extraClassPath", System.getProperty("java.class.path")) 
			  .set("spark.ui.enabled", Boolean.valueOf(VTL_SPARK_UI_ENABLED.getValue()).toString())
			  .set("spark.ui.port", Integer.valueOf(VTL_SPARK_UI_PORT.getValue()).toString());
		
		// Set SEQUENTIAL to avoid creating new threads while inside the executor
		System.setProperty("vtl.sequential", "true");
		
		session = SparkSession
			  .builder()
			  .config(conf)
			  .master(master)
			  .getOrCreate();
		
		reader = session.read();
	}
	
	public SparkEnvironment(SparkContext sc)
	{
		session = SparkSession
			  .builder()
			  .config(sc.getConf())
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
		
		String[] parts = name.split(":");
		if (parts.length != 2)
			return false;
		
		return true;
	}

	@Override
	public Optional<VTLValue> getValue(MetadataRepository repo, String name)
	{
		if (!contains(name))
			return Optional.empty();

		SparkDataSet dataset = null;
		String[] parts = name.split(":");
		switch (parts[0])
		{
			case "csv":
				dataset = inferSchema(repo, reader.format("csv").option("header", "true").load(parts[1]), name); 
				break;
			default: 
				dataset = inferSchema(repo, reader.format(parts[0]).load(parts[1]), name); 
				break;
		}
		
		frames.put(name, dataset);
		return Optional.of(frames.get(name));
	}
	
	@Override
	public boolean store(VTLValue value, String alias)
	{
		alias = alias.matches("'.*'") ? alias.replaceAll("'(.*)'", "$1") : alias.toLowerCase();
		if (!(value instanceof DataSet) || !alias.startsWith("spark:"))
			return false;
		
		alias = alias.substring(6);
		String[] parts = alias.split(":", 2);
		if (parts.length != 2)
			return false;
		
		final DataSetMetadata metadata = ((DataSet) value).getMetadata();
		final SparkDataSet dataSet = value instanceof SparkDataSet ? (SparkDataSet) value : new SparkDataSet(session, metadata, (DataSet) value);
		Dataset<Row> dataFrame = dataSet.getDataFrame();
		
		try
		{
			LOGGER.info("Writing {} file {}...", parts[0], parts[1]);
			
			// Add metadata in case it was lost
			for (String name: dataFrame.columns())
			{
				final Optional<DataStructureComponent<?, ?, ?>> component = metadata.getComponent(name);
				if (component.isPresent())
					dataFrame = dataFrame.withColumn(name, dataFrame.col(name).as(name, getMetadataFor(component.get())));
			}
			
			dataFrame.drop("$lineage$")
				.write()
				.format(parts[0])
				.mode(Overwrite)
				.option("header","true")
				.save(parts[1]);

			LOGGER.debug("Finished writing {} file {}...", parts[0], parts[1]);
			return true;
		}
		catch (RuntimeException e)
		{
			LOGGER.error("Error saving Spark dataframe " + alias, e);
			return false;
		}
	}
	
	private SparkDataSet inferSchema(MetadataRepository repo, Dataset<Row> sourceDataFrame, String alias)
	{
		StructType schema = sourceDataFrame.schema();
		
		if (schema.forall(field -> !(field.dataType() instanceof StructType || field.dataType() instanceof ArrayType) && field.metadata().contains("Role")))
		{
			// infer structure from the schema metadata
			DataSetMetadata structure = new DataStructureBuilder().addComponents(getComponentsFromStruct(repo, schema)).build();
			Column[] names = getColumnsFromComponents(structure).toArray(new Column[structure.size()]);
			Column lineage = new Column(Literal.create(LineageSparkUDT.serialize(LineageExternal.of("spark:" + alias)), LineageSparkUDT));
			return new SparkDataSet(session, new DataPointEncoder(structure), sourceDataFrame.select(names).withColumn("$lineage$", lineage));
		}
		else if (!schema.forall(field -> field.dataType() instanceof StringType))
		{
			// infer structure from the schema field types
			LOGGER.warn("Reading a non-csv file missing VTL metadata, the inferred schema may not be exact.");
			Dataset<Row> sourceDataFrame2 = sourceDataFrame;
			
			for (StructField field: schema.fields())
				if (field.dataType() instanceof TimestampType)
					sourceDataFrame2 = sourceDataFrame2.withColumn(field.name(), to_date(sourceDataFrame2.col(field.name())));
				else if (field.dataType() instanceof IntegerType)
					sourceDataFrame2 = sourceDataFrame2.withColumn(field.name(), sourceDataFrame2.col(field.name()).cast(LongType));
			
			DataSetMetadata structure = new DataStructureBuilder().addComponents(getComponentsFromStruct(repo, sourceDataFrame2.schema())).build();
			Column[] names = getColumnsFromComponents(structure).toArray(new Column[structure.size()]);
			Column lineage = new Column(Literal.create(LineageSparkUDT.serialize(LineageExternal.of("spark:" + alias)), LineageSparkUDT));
			Dataset<Row> enriched = sourceDataFrame2.select(names).withColumn("$lineage$", lineage);
			return new SparkDataSet(session, new DataPointEncoder(structure), enriched);
		}
		else
		{
			LOGGER.debug("Using CSV header because scheme is missing metadata: {}", schema);
			// infer structure from the column header names
	
			String[] fieldNames = schema.fieldNames();
			Entry<List<DataStructureComponent<?, ?, ?>>, Map<DataStructureComponent<?, ?, ?>, String>> metaInfo = extractMetadata(null, fieldNames);
			DataSetMetadata structure = new DataStructureBuilder(metaInfo.getKey()).build();
			
			// masks for decoding CSV rows
			Map<DataStructureComponent<?, ?, ?>, String> masks = metaInfo.getValue();
			
			// normalized column names in alphabetical order
			Map<String, String> newToOldNames = IntStream.range(0, fieldNames.length)
					.mapToObj(i -> new SimpleEntry<>(metaInfo.getKey().get(i).getVariable().getName(), fieldNames[i]))
					.collect(entriesToMap());
			String[] normalizedNames = newToOldNames.keySet().toArray(new String[newToOldNames.size()]);
			Arrays.sort(normalizedNames, 0, normalizedNames.length);
			
			// Array of parsers for CSV fields (strings) into scalars
			Map<DataStructureComponent<?, ?, ?>, DataType> types = structure.stream()
				.collect(toMapWithValues(c -> getDataTypeFor(c)));
			Column[] converters = Arrays.stream(normalizedNames, 0, normalizedNames.length)
					.map(structure::getComponent)
					.map(Optional::get)
					.sorted(DataStructureComponent::byNameAndRole)
					.map(c -> udf(repr -> mapValue(c, repr.toString(), masks.get(c)).get(), types.get(c))
							.apply(sourceDataFrame.col(newToOldNames.get(c.getVariable().getName())))
							.as(c.getVariable().getName(), getMetadataFor(c)))
					.collect(toList())
					.toArray(new Column[normalizedNames.length + 1]);
			
			// add a column and a converter for the lineage
			byte[] serializedLineage = LineageSparkUDT.serialize(LineageExternal.of("spark:" + alias));
			converters[converters.length - 1] = lit(serializedLineage).alias("$lineage$");
	
			Dataset<Row> converted = sourceDataFrame.select(converters);
			Column[] ids = getColumnsFromComponents(structure.getIDs()).toArray(new Column[0]);
			return new SparkDataSet(session, structure, converted.repartition(ids));
		}
	}
}
