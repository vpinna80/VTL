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
import static scala.collection.JavaConverters.asJava;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.internal.SQLConf;
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
import it.bancaditalia.oss.vtl.impl.types.data.Frequency;
import it.bancaditalia.oss.vtl.impl.types.data.date.MonthPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.QuarterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.SemesterPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.TimeRangeHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.YearPeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageCall;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageExternal;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageImpl;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageSet;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
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
	public static final VTLProperty VTL_SPARK_SEARCH_PATH = 
			new VTLPropertyImpl("vtl.spark.search.path", "Path to search for spark files", System.getenv("VTL_PATH"), EnumSet.of(REQUIRED), System.getenv("VTL_PATH"));

	/* package */ static final LineageSparkUDT LineageSparkUDT = new LineageSparkUDT();
	private static final AtomicReference<SQLConf> MASTER_CONF = new AtomicReference<>();
	private static final ThreadLocal<Boolean> CONFS = new ThreadLocal<>();
	
	static
	{
		registerSupportedProperties(SparkEnvironment.class, VTL_SPARK_MASTER_CONNECTION, VTL_SPARK_UI_ENABLED, VTL_SPARK_UI_PORT, VTL_SPARK_PAGE_SIZE, VTL_SPARK_SEARCH_PATH);
		if (!UDTRegistration.exists(Lineage.class.getName()))
		{
			List<Class<?>> lClasses = List.of(LineageExternal.class, LineageCall.class, LineageNode.class, LineageImpl.class, LineageSet.class, Lineage.class);
			for (Class<?> lineageClass: lClasses)
				UDTRegistration.register(lineageClass.getName(), LineageSparkUDT.class.getName());
			UDTRegistration.register(Frequency.class.getName(), FrequencySparkUDT.class.getName());
			UDTRegistration.register(TimeRangeHolder.class.getName(), TimeRangeSparkUDT.class.getName());
			List<Class<?>> pClasses = List.of(YearPeriodHolder.class, SemesterPeriodHolder.class, QuarterPeriodHolder.class, MonthPeriodHolder.class, PeriodHolder.class);
			for (Class<?> periodClass: pClasses)
				UDTRegistration.register(periodClass.getName(), TimePeriodSparkUDT.class.getName());
		}
	}

	public static void startContextCleaner()
	{
		
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
	private final Map<VTLAlias, SparkDataSet> frames = new ConcurrentHashMap<>();
	private final DataFrameReader reader;
	private final List<Path> paths;

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
			  /* enable for DEBUG
			  .set("spark.sql.codegen.wholeStage", "false")
			  .set("spark.sql.codegen", "false")
			  .set("spark.sql.codegen.factoryMode", "NO_CODEGEN")
			  //*/
			  .set("spark.sql.windowExec.buffer.in.memory.threshold", "16384")
			  .set("spark.sql.caseSensitive", "true")
			  .set("spark.executor.extraClassPath", System.getProperty("java.class.path")) 
			  .set("spark.ui.enabled", Boolean.valueOf(VTL_SPARK_UI_ENABLED.getValue()).toString())
			  .set("spark.ui.port", Integer.valueOf(VTL_SPARK_UI_PORT.getValue()).toString());
		
		// Set SEQUENTIAL to avoid creating new threads while inside the executor
		paths = VTL_SPARK_SEARCH_PATH.getValues().stream().map(Paths::get).collect(toList());
		
		session = SparkSession
			  .builder()
			  .config(conf)
			  .master(master)
			  .getOrCreate();
		
		SQLConf sqlConf = session.sessionState().conf();
		if (MASTER_CONF.compareAndSet(null, sqlConf))
			CONFS.set(true);
		
		reader = session.read();
	}
	
	public SparkEnvironment(List<Path> paths)
	{
		LOGGER.info("Connecting to Spark master {}", "local[4]");
		SparkConf conf = new SparkConf()
			  .setMaster("local[4]")
			  .setAppName("Spark SQL Environment for VTL Engine [" + hashCode() + "]")
			  .set("spark.executor.processTreeMetrics.enabled", "false")
			  .set("spark.kryo.registrator", "it.bancaditalia.oss.vtl.impl.environment.spark.SparkEnvironment$VTLKryoRegistrator")
			  .set("spark.sql.datetime.java8API.enabled", "true")
			  .set("spark.sql.catalyst.dateType", "Instant")
			  .set("spark.sql.windowExec.buffer.in.memory.threshold", "16384")
			  .set("spark.sql.caseSensitive", "true")
			  .set("spark.executor.extraClassPath", System.getProperty("java.class.path")) 
			  .set("spark.ui.enabled", "false");
		
		// Set SEQUENTIAL to avoid creating new threads while inside the executor
		this.paths = paths;
		
		session = SparkSession
			  .builder()
			  .config(conf)
			  .master("local[4]")
			  .getOrCreate();
		
		SQLConf sqlConf = session.sessionState().conf();
		if (MASTER_CONF.compareAndSet(null, sqlConf))
			CONFS.set(true);
		
		reader = session.read();
	}
	
	static void ensureConf(SparkSession session)
	{
		if (CONFS.get() != Boolean.TRUE)
		{
			CONFS.set(Boolean.TRUE);
			
			SQLConf master = MASTER_CONF.get();
			SQLConf sqlConf = SQLConf.get();
			Map<String, String> javaMasterConf = asJava(master.getAllConfs());
			
			if (!javaMasterConf.equals(asJava(sqlConf.getAllConfs())))
				for (String k: javaMasterConf.keySet())
					sqlConf.setConfString(k, master.getConfString(k));
		}
	}

	@Override
	public Optional<VTLValue> getValue(MetadataRepository repo, VTLAlias alias)
	{
		if (frames.containsKey(alias))
			return Optional.of(frames.get(alias));
		
		String source = repo.getDatasetSource(alias);
		if (!source.startsWith("spark:") || source.substring(6).indexOf(':') == -1)
			return Optional.empty();
		Path sourcePath = Paths.get(source.substring(6).split(":", 2)[1]);
		String type = source.substring(6).split(":", 2)[0];
		
		SparkDataSet dataset = null;
		
		Path file = paths.stream()
				.map(path -> path.resolve(sourcePath))
				.filter(Files::exists)
				.limit(1)
				.peek(path -> LOGGER.info("Found {} in {}", sourcePath, path))
				.findAny()
				.orElseThrow(() -> new InvalidParameterException("Cannot find " + type + " file in Spark search path: " + sourcePath));
		
		if (!Files.isRegularFile(file) || !Files.isReadable(file))
			throw new InvalidParameterException("File is not a readable: " + file);
		
		DataFrameReader formatted = reader.format(type);
		if ("csv".equals(type))
			formatted = formatted.option("header", "true");
		
		Optional<DataSetMetadata> maybeStructure = repo.getStructure(alias);
		if (maybeStructure.isPresent())
		{
			DataSetMetadata structure = maybeStructure.get();
			Dataset<Row> applied = SparkUtils.applyStructure(structure, formatted.load(file.toString()));
			Column lineage = new Column(Literal.create(LineageSparkUDT.serialize(LineageExternal.of("spark:" + alias)), LineageSparkUDT));
			dataset = new SparkDataSet(session, structure, new DataPointEncoder(session, structure), applied.withColumn("$lineage$", lineage));
		}
		else
			dataset = inferSchema(repo, formatted.load(file.toString()), alias); 
		frames.put(alias, dataset);
		return Optional.of(dataset);
	}
	
	@Override
	public boolean store(VTLValue value, VTLAlias alias)
	{
		if (!(value instanceof DataSet) || !alias.getName().startsWith("spark:"))
			return false;
		
		String[] parts = alias.getName().substring(6).split(":", 2);
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
				final Optional<DataStructureComponent<?, ?, ?>> component = metadata.getComponent(VTLAliasImpl.of(name));
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
	
	private SparkDataSet inferSchema(MetadataRepository repo, Dataset<Row> sourceDataFrame, VTLAlias alias)
	{
		Column lineage = new Column(Literal.create(LineageSparkUDT.serialize(LineageExternal.of("spark:" + alias)), LineageSparkUDT));
		StructType schema = sourceDataFrame.schema();
		
		if (schema.forall(field -> !(field.dataType() instanceof StructType || field.dataType() instanceof ArrayType) && field.metadata().contains("Role")))
		{
			// infer structure from the schema metadata
			DataSetMetadata structure = new DataStructureBuilder().addComponents(getComponentsFromStruct(repo, schema)).build();
			Column[] names = getColumnsFromComponents(structure).toArray(new Column[structure.size()]);
			return new SparkDataSet(session, structure, new DataPointEncoder(session, structure), sourceDataFrame.select(names).withColumn("$lineage$", lineage));
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
			Dataset<Row> enriched = sourceDataFrame2.select(names).withColumn("$lineage$", lineage);
			return new SparkDataSet(session, structure, new DataPointEncoder(session, structure), enriched);
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
			Map<VTLAlias, String> newToOldNames = IntStream.range(0, fieldNames.length)
					.mapToObj(i -> new SimpleEntry<>(metaInfo.getKey().get(i).getVariable().getAlias(), fieldNames[i]))
					.collect(entriesToMap());
			VTLAlias[] normalizedNames = newToOldNames.keySet().toArray(new VTLAlias[newToOldNames.size()]);
			Arrays.sort(normalizedNames, 0, normalizedNames.length);
			
			// Array of parsers for CSV fields (strings) into scalars
			Map<DataStructureComponent<?, ?, ?>, DataType> types = structure.stream()
				.collect(toMapWithValues(c -> getDataTypeFor(c)));
			Column[] converters = Arrays.stream(normalizedNames, 0, normalizedNames.length)
					.map(structure::getComponent)
					.map(Optional::get)
					.sorted(DataStructureComponent::byNameAndRole)
					.map(c -> udf(repr -> mapValue(c, repr.toString(), masks.get(c)).get(), types.get(c))
							.apply(sourceDataFrame.col(newToOldNames.get(c.getVariable().getAlias())))
							.as(c.getVariable().getAlias().getName(), getMetadataFor(c)))
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
