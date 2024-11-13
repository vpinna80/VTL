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

import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.isUseBigDecimal;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkEnvironment.LineageSparkUDT;
import static it.bancaditalia.oss.vtl.impl.environment.util.CSVParseUtils.stringToTime;
import static it.bancaditalia.oss.vtl.impl.environment.util.CSVParseUtils.stringToTimePeriod;
import static it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl.createNumberValue;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DURATIONDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIME_PERIODDS;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.DEFAULT_JAVA_DECIMAL_ENCODER;
import static org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.STRICT_LOCAL_DATE_ENCODER;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static scala.collection.JavaConverters.asJava;
import static scala.jdk.javaapi.CollectionConverters.asScala;

import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.JavaTypeInference;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.EncoderField;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.IterableEncoder;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.MapEncoder;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.ProductEncoder;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import it.bancaditalia.oss.vtl.impl.types.data.BaseScalarValue;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.Frequency;
import it.bancaditalia.oss.vtl.impl.types.data.GenericTimeValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.TimeRangeHolder;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.Component.ViralAttribute;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.DateDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.DurationDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.GenericTuple;
import it.bancaditalia.oss.vtl.util.SerDoubleSumAvgCount;
import it.bancaditalia.oss.vtl.util.SerFunction;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.Seq;
import scala.reflect.ClassTag;

public class SparkUtils
{
	private static final Map<ValueDomainSubset<?, ?>, AgnosticEncoder<?>> DOMAIN_ENCODERS = new ConcurrentHashMap<>();
	private static final Map<ValueDomainSubset<?, ?>, DataType> DOMAIN_DATATYPES = new ConcurrentHashMap<>();
	private static final Map<ValueDomainSubset<?, ?>, SerFunction<Serializable, ScalarValue<?, ?, ?, ?>>> DOMAIN_BUILDERS = new ConcurrentHashMap<>();
	
	static final Map<Class<?>, SerFunction<Serializable, ScalarValue<?, ?, ?, ?>>> PRIM_BUILDERS = new ConcurrentHashMap<>();

	static
	{
		DOMAIN_ENCODERS.put(BOOLEANDS, AgnosticEncoders.BoxedBooleanEncoder$.MODULE$);
		DOMAIN_ENCODERS.put(STRINGDS, AgnosticEncoders.StringEncoder$.MODULE$);
		DOMAIN_ENCODERS.put(INTEGERDS, AgnosticEncoders.BoxedLongEncoder$.MODULE$);
		DOMAIN_ENCODERS.put(NUMBERDS, isUseBigDecimal() ? DEFAULT_JAVA_DECIMAL_ENCODER() : AgnosticEncoders.BoxedDoubleEncoder$.MODULE$);
		DOMAIN_ENCODERS.put(DURATIONDS, AgnosticEncoders.BoxedIntEncoder$.MODULE$);
		DOMAIN_ENCODERS.put(TIMEDS, TimeRangeSparkUDT.getEncoder());
		DOMAIN_ENCODERS.put(TIME_PERIODDS, TimePeriodSparkUDT.getEncoder());
		DOMAIN_ENCODERS.put(DATEDS, STRICT_LOCAL_DATE_ENCODER());

		DOMAIN_DATATYPES.put(BOOLEANDS, BooleanType);
		DOMAIN_DATATYPES.put(STRINGDS, StringType);
		DOMAIN_DATATYPES.put(INTEGERDS, LongType);
		DOMAIN_DATATYPES.put(NUMBERDS, isUseBigDecimal() ? DecimalType.USER_DEFAULT() : DoubleType);
		DOMAIN_DATATYPES.put(DURATIONDS, new FrequencySparkUDT());
		DOMAIN_DATATYPES.put(TIMEDS, new TimeRangeSparkUDT());
		DOMAIN_DATATYPES.put(TIME_PERIODDS, new TimePeriodSparkUDT());
		DOMAIN_DATATYPES.put(DATEDS, DateType);

		DOMAIN_BUILDERS.put(BOOLEANDS, v -> BooleanValue.of((Boolean) v));
		DOMAIN_BUILDERS.put(STRINGDS, v -> StringValue.of((String) v));
		DOMAIN_BUILDERS.put(INTEGERDS, v -> IntegerValue.of((Long) v));
		DOMAIN_BUILDERS.put(NUMBERDS, v -> createNumberValue((Number) v));
		DOMAIN_BUILDERS.put(DURATIONDS, v -> v == null ? NullValue.instance(DURATIONDS) : ((Frequency) v).get());
		DOMAIN_BUILDERS.put(TIMEDS, v -> GenericTimeValue.of((TimeRangeHolder) v));
		DOMAIN_BUILDERS.put(TIME_PERIODDS, v -> TimePeriodValue.of((PeriodHolder<?>) v));
		DOMAIN_BUILDERS.put(DATEDS, v -> DateValue.of((LocalDate) v));

		PRIM_BUILDERS.put(Boolean.class, v -> BooleanValue.of((Boolean) v));
		PRIM_BUILDERS.put(String.class, v -> StringValue.of((String) v));
		PRIM_BUILDERS.put(Long.class, v -> IntegerValue.of((Long) v));
		PRIM_BUILDERS.put(Double.class, v -> createNumberValue((Number) v));
		PRIM_BUILDERS.put(LocalDate.class, v -> DateValue.of((LocalDate) v));
	}

	public static Set<DataStructureComponent<?, ?, ?>> getComponentsFromStruct(MetadataRepository repo, StructType schema)
	{
		Set<DataStructureComponent<?, ?, ?>> result = new HashSet<>();
		for (StructField field: schema.fields())
			result.add(getComponentFor(repo, field));
		
		return result;
	}

	private static DataStructureComponent<? extends Component, ?, ?> getComponentFor(MetadataRepository repo, StructField field)
	{
		Class<? extends Component> role;
		switch ((int) field.metadata().getLong("Role"))
		{
			case 1: role = Identifier.class; break;
			case 2: role = Measure.class; break;
			case 3: role = Attribute.class; break;
			case 4: role = ViralAttribute.class; break;
			default: throw new UnsupportedOperationException("No VTL role corresponding to metadata.");
		}
		
		return repo.getVariable(VTLAliasImpl.of(field.name())).as(role);
	}

	public static Dataset<Row> applyStructure(DataSetMetadata structure, Dataset<Row> sourceDataFrame)
	{
		Column[] names = getColumnsFromComponents(structure).toArray(new Column[structure.size()]);
		
		for (DataStructureComponent<?, ?, ?> comp: structure)
		{
			ValueDomainSubset<?, ?> domain = comp.getVariable().getDomain();
			String name = comp.getVariable().getAlias().getName();
			if (domain instanceof IntegerDomainSubset)
				sourceDataFrame = sourceDataFrame.withColumn(name, sourceDataFrame.col(name).cast(LongType));
			else if (domain instanceof NumberDomainSubset)
				sourceDataFrame = sourceDataFrame.withColumn(name, sourceDataFrame.col(name).cast(DoubleType));
			else if (domain instanceof BooleanDomainSubset)
				sourceDataFrame = sourceDataFrame.withColumn(name, sourceDataFrame.col(name).cast(BooleanType));
			else if (domain instanceof StringDomainSubset)
				; // nothing
			else if (domain instanceof DateDomainSubset)
				sourceDataFrame = sourceDataFrame.withColumn(name, to_date(sourceDataFrame.col(name)));
			else if (domain instanceof TimePeriodDomainSubset)
				sourceDataFrame = sourceDataFrame.withColumn(name, 
						udf(s -> stringToTimePeriod((String) s).get(), new TimePeriodSparkUDT()).apply(sourceDataFrame.col(name)));
			else if (domain instanceof TimeDomainSubset)
				sourceDataFrame = sourceDataFrame.withColumn(name, 
						udf(s -> stringToTime((String) s).get(), new TimeRangeSparkUDT()).apply(sourceDataFrame.col(name)));
			else if (domain instanceof DurationDomainSubset)
				sourceDataFrame = sourceDataFrame.withColumn(name, 
						udf(s -> Frequency.valueOf((String) s), new FrequencySparkUDT()).apply(sourceDataFrame.col(name)));
			else
				throw new UnsupportedOperationException("Unsupported domain " + domain);
		}
		
		return sourceDataFrame.select(names);
	}
	
	public static ScalarValue<?, ?, ?, ?> getScalarFor(DataStructureComponent<?, ?, ?> component, Serializable serialized)
	{
		SerFunction<Serializable, ScalarValue<?, ?, ?, ?>> builder = null;
		ValueDomainSubset<?, ?> domain;
		for (domain = component.getVariable().getDomain(); domain != null && builder == null; domain = (ValueDomainSubset<?, ?>) domain.getParentDomain()) 
			builder = DOMAIN_BUILDERS.get(domain);
		
		if (builder == null)
			throw new UnsupportedOperationException("Unsupported decoding of domain " + component.getVariable().getDomain());

		domain = component.getVariable().getDomain();
		DOMAIN_BUILDERS.putIfAbsent(domain, builder);

		if (serialized instanceof Date)
			throw new UnsupportedOperationException("Found java.sql.Date " + serialized + " for component " + component);
		
		return domain.cast(builder.apply(serialized));
	}
	
	public static Metadata getMetadataFor(DataStructureComponent<?, ?, ?> component)
	{
		MetadataBuilder metadataBuilder = new MetadataBuilder();
		if (component.getRole() == Identifier.class)
			metadataBuilder.putLong("Role", 1);
		else if (component.getRole() == Measure.class)
			metadataBuilder.putLong("Role", 2);
		else if (component.getRole() == Attribute.class)
			metadataBuilder.putLong("Role", 3);
		else if (component.getRole() == ViralAttribute.class)
			metadataBuilder.putLong("Role", 4);
		else
			throw new IllegalStateException("Unqualified role class: " + component.getRole());
		
		return metadataBuilder.putString("Domain", component.getVariable().getDomain().getAlias().getName()).build();
	}

	public static StructField getFieldFor(DataStructureComponent<?, ?, ?> component)
	{
		DataType type = getDataTypeFor(component);
		Metadata metadata = getMetadataFor(component);
		
		return new StructField(component.getVariable().getAlias().getName(), type, component.is(NonIdentifier.class), metadata);
	}

	public static EncoderField getEncoderFieldFor(DataStructureComponent<?, ?, ?> component)
	{
		return new EncoderField(component.getVariable().getAlias().getName(), DOMAIN_ENCODERS.get(component.getVariable().getDomain()), 
				true, getMetadataFor(component), Option.empty(), Option.empty());
	}

	public static DataType getDataTypeFor(DataStructureComponent<?, ?, ?> component)
	{
		ValueDomainSubset<?, ?> domain = component.getVariable().getDomain();
		if (domain instanceof StringDomain)
			return StringType;
		else if (DOMAIN_DATATYPES.containsKey(domain))
			return DOMAIN_DATATYPES.get(domain);
		else
			throw new UnsupportedOperationException("Domain " + domain + " is not supported on Spark.");
	}

	public static List<StructField> createStructFromComponents(DataStructureComponent<?, ?, ?>[] components)
	{
		return structHelper(Arrays.stream(components), SparkUtils::getFieldFor);
	}

	public static List<EncoderField> createFieldFromComponents(DataStructureComponent<?, ?, ?>[] components)
	{
		return structHelper(Arrays.stream(components), SparkUtils::getEncoderFieldFor);
	}

	public static List<StructField> createStructFromComponents(Collection<? extends DataStructureComponent<?, ?, ?>> components)
	{
		return structHelper(components.stream(), SparkUtils::getFieldFor);
	}	

	public static List<String> getNamesFromComponents(Collection<? extends DataStructureComponent<?, ?, ?>> components)
	{
		return structHelper(components.stream(), c -> c.getVariable().getAlias().getName());
	}

	public static List<Column> getColumnsFromComponents(Collection<? extends DataStructureComponent<?, ?, ?>> components)
	{
		return structHelper(components.stream(), c -> col(c.getVariable().getAlias().getName()));
	}

	public static <F> List<F> structHelper(Stream<? extends DataStructureComponent<?, ?, ?>> stream, SerFunction<? super DataStructureComponent<?, ?, ?>, F> mapper)
	{
		return stream
			.sorted(DataStructureComponent::byNameAndRole)
			.map(mapper)
			.collect(toList());
	}

	public static <T> Encoder<T> getEncoderFor(Serializable instance, ValueDomainSubset<?, ?> domain, DataSetMetadata structure)
	{
		AgnosticEncoder<?> resultEncoder; 
		
		String className = instance.getClass().getSimpleName();
		if ("PartitionToRank".equals(className))
		{
			DataStructureComponent<?, ?, ?>[] components = structure.toArray(new DataStructureComponent<?, ?, ?>[structure.size()]);
			Arrays.sort(components, DataStructureComponent::byNameAndRole);
			List<EncoderField> fields = new ArrayList<>(createFieldFromComponents(components));
			fields.add(new EncoderField("$lineage$", new AgnosticEncoders.UDTEncoder<>(LineageSparkUDT, LineageSparkUDT.class), false, Metadata.empty(), Option.empty(), Option.empty()));
			resultEncoder = new AgnosticEncoders.RowEncoder(asScala((Iterable<EncoderField>) fields).toSeq());
		}
		else if ("RankedPartition".equals(className))
		{
			try
			{
				Class<?>[] repr = (Class<?>[]) instance.getClass().getField("repr").get(instance);
				List<EncoderField> fieldEncoders = new ArrayList<>(); 
				for (int i = 0; i < repr.length; i++)
					fieldEncoders.add(new EncoderField("get" + (i + 1), JavaTypeInference.encoderFor(repr[i]), 
							true, new Metadata(), Option.apply("get" + (i + 1)), Option.apply("set" + (i + 1))));
				
				Seq<EncoderField> seq = asScala((Iterable<EncoderField>) fieldEncoders).toSeq();
				ProductEncoder<GenericTuple> keyEncoder = new ProductEncoder<>(ClassTag.apply(GenericTuple.class), seq, Option.empty());
				AgnosticEncoder<Long> longEncoder = JavaTypeInference.encoderFor(Long.class);
				resultEncoder = new MapEncoder<>(ClassTag.apply(HashMap.class), keyEncoder, longEncoder, true);
			}
			catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException e)
			{
				throw new IllegalStateException(e);
			}
		}
		else if ("SerDoubleSumAvgCount".equals(className))
		{
			List<EncoderField> fieldEncoders = new ArrayList<>(); 
			fieldEncoders.add(new EncoderField("getCount", JavaTypeInference.encoderFor(int.class), true, new Metadata(), Option.empty(), Option.empty()));
			fieldEncoders.add(new EncoderField("getSums", JavaTypeInference.encoderFor(double[].class), true, new Metadata(), Option.empty(), Option.empty()));
			Seq<EncoderField> seq = asScala((Iterable<EncoderField>) fieldEncoders).toSeq();
			resultEncoder = new ProductEncoder<>(ClassTag.apply(SerDoubleSumAvgCount.class), seq, Option.empty());
		}
		else if ("ListOfDateValues".equals(className))
		{
			List<EncoderField> fieldEncoders = new ArrayList<>();
			fieldEncoders.add(new EncoderField("get", STRICT_LOCAL_DATE_ENCODER(), true, new Metadata(), Option.empty(), Option.empty()));
			Seq<EncoderField> seq = asScala((Iterable<EncoderField>) fieldEncoders).toSeq();
			resultEncoder = new IterableEncoder<>(ClassTag.apply(ArrayList.class), new ProductEncoder<>(ClassTag.apply(DateValue.class), seq, Option.empty()), true, false);
		}
		else if ("Holder".equals(className))
		{
			try
			{
				List<EncoderField> fieldEncoders = new ArrayList<>();
				Class<?> holdClass = (Class<?>) instance.getClass().getField("repr").get(instance);
				AgnosticEncoder<?> vEncoder;
				if (BaseScalarValue.class.isAssignableFrom(holdClass))
				{
					vEncoder = DOMAIN_ENCODERS.get(domain);
					if (vEncoder == null)
						throw new UnsupportedOperationException(domain.toString());
				}
				else
					vEncoder = JavaTypeInference.encoderFor(holdClass);
				fieldEncoders.add(new EncoderField("get", vEncoder, true, new Metadata(), Option.empty(), Option.empty()));
				Seq<EncoderField> seq = asScala((Iterable<EncoderField>) fieldEncoders).toSeq();
				resultEncoder = new ProductEncoder<>(ClassTag.apply(SerDoubleSumAvgCount.class), seq, Option.empty());
			}
			catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException e)
			{
				throw new IllegalStateException(e);
			}
		}
		else if (instance instanceof BaseScalarValue)
		{
			resultEncoder = DOMAIN_ENCODERS.get(domain);
			if (resultEncoder == null)
				throw new UnsupportedOperationException(domain.toString());
		}
		else if (instance instanceof Object[])
		{
			resultEncoder = JavaTypeInference.encoderFor(instance.getClass());
		}
		else if (instance instanceof AtomicReference)
		{
			resultEncoder = JavaTypeInference.encoderFor(instance.getClass());
		}
		else
			throw new IllegalStateException(instance.getClass().getName());
		
		@SuppressWarnings("unchecked")
		Encoder<T> encoder = (Encoder<T>) ExpressionEncoder.apply(resultEncoder);
		return encoder;
	}

	@SuppressWarnings("unchecked")
	public static <TT extends Serializable> TT reinterpret(DataStructureComponent<?, ?, ?> comp, Serializable serAcc)
	{
		if (serAcc == null)
			return (TT) NullValue.instanceFrom(comp);
		else if (serAcc instanceof Row)
			return (TT) serAcc;
		else if (PRIM_BUILDERS.containsKey(serAcc.getClass()))
			return (TT) PRIM_BUILDERS.get(serAcc.getClass()).apply(serAcc);
		else if (serAcc instanceof scala.collection.immutable.Map)
		{
			Map<GenericTuple, Long> rankedPartition = new HashMap<>();
			scala.collection.Iterator<? extends Tuple2<?, ?>> iterator = ((scala.collection.immutable.Map<?, ?>) serAcc).iterator();
			while (iterator.hasNext())
			{
				Tuple2<?, ?> entry = iterator.next();
				
				Serializable[] values = asJava(((Row) entry._1).toSeq()).toArray(Serializable[]::new);
				rankedPartition.put(new GenericTuple(values), (Long) entry._2);
			}
			
			return (TT) rankedPartition;
		}
		else if (serAcc instanceof scala.collection.immutable.ArraySeq)
		{
			SerFunction<Serializable, ScalarValue<?, ?, ?, ?>> builder = DOMAIN_BUILDERS.getOrDefault(comp.getVariable().getDomain(), ScalarValue.class::cast);
			ArrayList<Serializable> list = new ArrayList<>();
			for (Object value: asJava((scala.collection.Iterable<?>) serAcc))
				list.add(builder.apply((Serializable) ((Row) value).get(0)));
			return (TT) list;
		}
		else		
			return (TT) serAcc;
	}

	private SparkUtils()
	{
	}
}
