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
import static it.bancaditalia.oss.vtl.impl.environment.util.CSVParseUtils.mapValue;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DURATIONDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIME_PERIODDS;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toArray;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.DEFAULT_JAVA_DECIMAL_ENCODER;
import static org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.STRICT_LOCAL_DATE_ENCODER;
import static org.apache.spark.sql.functions.col;
import static scala.collection.JavaConverters.asJava;
import static scala.jdk.javaapi.CollectionConverters.asScala;

import java.io.Serializable;
import java.sql.Date;
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

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.JavaTypeInference;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.EncoderField;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.IterableEncoder;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.MapEncoder;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.ProductEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UserDefinedType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.types.data.BaseScalarValue;
import it.bancaditalia.oss.vtl.impl.types.data.BigDecimalValue;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.DurationValue;
import it.bancaditalia.oss.vtl.impl.types.data.GenericTimeValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.Component.ViralAttribute;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.GenericTuple;
import it.bancaditalia.oss.vtl.util.Holder;
import it.bancaditalia.oss.vtl.util.SerDoubleSumAvgCount;
import it.bancaditalia.oss.vtl.util.SerFunction;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.Seq;
import scala.reflect.ClassTag;

public class SparkUtils
{
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(SparkUtils.class);
	
	@SuppressWarnings("rawtypes")
	private static final Map<Class<? extends ScalarValue>, AgnosticEncoder<?>> SCALAR_ENCODERS = new ConcurrentHashMap<>();
	private static final Map<ValueDomainSubset<?, ?>, UserDefinedType<?>> DOMAIN_DATATYPES = new ConcurrentHashMap<>();
	
	static
	{
		SCALAR_ENCODERS.put(BooleanValue.class, AgnosticEncoders.BoxedBooleanEncoder$.MODULE$);
		SCALAR_ENCODERS.put(StringValue.class, AgnosticEncoders.StringEncoder$.MODULE$);
		SCALAR_ENCODERS.put(IntegerValue.class, AgnosticEncoders.BoxedLongEncoder$.MODULE$);
		SCALAR_ENCODERS.put(DoubleValue.class, AgnosticEncoders.BoxedDoubleEncoder$.MODULE$);
		SCALAR_ENCODERS.put(BigDecimalValue.class, DEFAULT_JAVA_DECIMAL_ENCODER());
		SCALAR_ENCODERS.put(DurationValue.class, AgnosticEncoders.BoxedIntEncoder$.MODULE$);
		SCALAR_ENCODERS.put(GenericTimeValue.class, GenericTimeValueUDT.ENCODER);
		SCALAR_ENCODERS.put(TimePeriodValue.class, TimePeriodValueUDT.ENCODER);
		SCALAR_ENCODERS.put(DateValue.class, STRICT_LOCAL_DATE_ENCODER());

		DOMAIN_DATATYPES.put(BOOLEANDS, new BooleanValueUDT());
		DOMAIN_DATATYPES.put(STRINGDS, new StringValueUDT());
		DOMAIN_DATATYPES.put(INTEGERDS, new IntegerValueUDT());
		DOMAIN_DATATYPES.put(NUMBERDS, isUseBigDecimal() ? new BigDecimalValueUDT() : new DoubleValueUDT());
		DOMAIN_DATATYPES.put(DURATIONDS, new DurationValueUDT());
		DOMAIN_DATATYPES.put(TIMEDS, new GenericTimeValueUDT());
		DOMAIN_DATATYPES.put(TIME_PERIODDS, new TimePeriodValueUDT());
		DOMAIN_DATATYPES.put(DATEDS, new DateValueUDT());
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

	public static MapFunction<Row, Row> parseCSVStrings(DataSetMetadata structure, Lineage lineage, DataPointEncoder encoder)
	{
		DataStructureComponent<?, ?, ?>[] comps = encoder.components;
		ValueDomainSubset<?,?>[] domains = encoder.domains;
		String[] names = Arrays.stream(comps).map(c -> c.getVariable().getAlias().getName()).collect(toArray(new String[comps.length]));
		
		return srcRow -> {
			int size = srcRow.size() + 1;
			Serializable[] newCols = new Serializable[size];
			for (int i = 0; i < size - 1; i++)
			{
				newCols[i] = mapValue(domains[i], srcRow.getAs(names[i]), null);
				if (newCols[i] instanceof NullValue)
					newCols[i] = null;
			}
			
			newCols[size - 1] = lineage;
			return new GenericRow(newCols);
		};
	}
	
	public static ScalarValue<?, ?, ?, ?> getScalarFor(ValueDomainSubset<?, ?> domain, Serializable serialized)
	{
		if (serialized instanceof ScalarValue)
			return domain.cast((ScalarValue<?, ?, ?, ?>) serialized);
		else if (serialized != null && serialized.getClass() == Date.class)
			return domain.cast(DateValue.of(((Date) serialized).toLocalDate()));
		else
			throw new UnsupportedOperationException("Cannot determine the scalar value corresponding to " + serialized.getClass());
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

//	public static EncoderField getEncoderFieldFor(DataStructureComponent<?, ?, ?> component)
//	{
//		return new EncoderField(component.getVariable().getAlias().getName(), SCALAR_ENCODERS.get(component.getVariable().getDomain()), 
//				true, getMetadataFor(component), Option.empty(), Option.empty());
//	}

	public static DataType getDataTypeFor(DataStructureComponent<?, ?, ?> component)
	{
		ValueDomainSubset<?, ?> domain = component.getVariable().getDomain();
		if (DOMAIN_DATATYPES.containsKey(domain))
			return DOMAIN_DATATYPES.get(domain);
		else
			throw new UnsupportedOperationException("Domain " + domain + " is not supported on Spark.");
	}

//	public static List<EncoderField> createFieldFromComponents(DataStructureComponent<?, ?, ?>[] components)
//	{
//		return structHelper(Arrays.stream(components), SparkUtils::getEncoderFieldFor);
//	}

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

	/**
	 * Infers an encoder for a given instance to be used in window functions.
	 * If the instance is a ScalarValue, the encoder is from its domain.
	 * NOTE: The third parameter is optional and used for a specific case.
	 * 
	 * @param <T>
	 * @param instance
	 * @param structure
	 * @return
	 */
	public static <T> AgnosticEncoder<T> getEncoderFor(Serializable instance, DataSetMetadata structure)
	{
		AgnosticEncoder<?> resultEncoder; 
		
		String className = instance.getClass().getSimpleName();
//		if ("PartitionToRank".equals(className))
//		{
//			DataStructureComponent<?, ?, ?>[] components = structure.toArray(new DataStructureComponent<?, ?, ?>[structure.size()]);
//			Arrays.sort(components, DataStructureComponent::byNameAndRole);
//			List<EncoderField> fields = new ArrayList<>(createFieldFromComponents(components));
//			fields.add(new EncoderField("$lineage$", new AgnosticEncoders.UDTEncoder<>(LineageSparkUDT, LineageSparkUDT.class), false, Metadata.empty(), Option.empty(), Option.empty()));
//			resultEncoder = new AgnosticEncoders.RowEncoder(asScala((Iterable<EncoderField>) fields).toSeq());
//		}
//		else 
		if ("RankedPartition".equals(className))
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
				Class<?> heldClass = (Class<?>) instance.getClass().getField("repr").get(instance);
				AgnosticEncoder<?> vEncoder;
				if (BaseScalarValue.class.isAssignableFrom(heldClass))
				{
					vEncoder = SCALAR_ENCODERS.get(heldClass.asSubclass(ScalarValue.class));
					if (vEncoder == null)
						throw new UnsupportedOperationException(heldClass.getSimpleName());
				}
				else
					vEncoder = JavaTypeInference.encoderFor(heldClass);
				fieldEncoders.add(new EncoderField("get", vEncoder, true, new Metadata(), Option.empty(), Option.empty()));
				Seq<EncoderField> seq = asScala((Iterable<EncoderField>) fieldEncoders).toSeq();
				resultEncoder = new ProductEncoder<>(ClassTag.apply(Holder.class), seq, Option.empty());
			}
			catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException e)
			{
				throw new IllegalStateException(e);
			}
		}
		else if ("TimeWithFreq".equals(className))
		{
			List<EncoderField> fieldEncoders = new ArrayList<>(); 
			fieldEncoders.add(new EncoderField("freq", AgnosticEncoders.PrimitiveIntEncoder$.MODULE$, false, new Metadata(), Option.empty(), Option.empty()));
			fieldEncoders.add(new EncoderField("range", GenericTimeValueUDT.ENCODER, true, new Metadata(), Option.empty(), Option.empty()));
			fieldEncoders.add(new EncoderField("date", STRICT_LOCAL_DATE_ENCODER(), true, new Metadata(), Option.empty(), Option.empty()));
			fieldEncoders.add(new EncoderField("holder", TimePeriodValueUDT.ENCODER, true, new Metadata(), Option.empty(), Option.empty()));
			Seq<EncoderField> seq = asScala((Iterable<EncoderField>) fieldEncoders).toSeq();
			resultEncoder = AgnosticEncoders.RowEncoder$.MODULE$.apply(seq);
		}
		else if (instance instanceof NullValue)
		{
			ValueDomainSubset<?, ?> domain = ((NullValue<?, ?>) instance).getMetadata().getDomain();

			@SuppressWarnings("unchecked")
			UserDefinedType<T> type = (UserDefinedType<T>) DOMAIN_DATATYPES.get(domain);
			if (type == null)
				throw new UnsupportedOperationException(domain.toString());
			
			@SuppressWarnings("unchecked")
			Class<? extends UserDefinedType<?>> clazz = (Class<? extends UserDefinedType<?>>) type.getClass();
			return AgnosticEncoders.UDTEncoder$.MODULE$.apply(type, clazz);
		}
		else if (instance instanceof BaseScalarValue)
		{
			resultEncoder = SCALAR_ENCODERS.get(instance.getClass());
			if (resultEncoder == null)
				throw new UnsupportedOperationException(instance.getClass().getSimpleName());
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
		AgnosticEncoder<T> encoder = (AgnosticEncoder<T>) resultEncoder;
		return encoder;
	}

	@SuppressWarnings("unchecked")
	public static <TT extends Serializable> TT reinterpret(DataStructureComponent<?, ?, ?> comp, Serializable serAcc)
	{
		if (serAcc instanceof ScalarValue)
			return (TT) serAcc;
		else if (serAcc instanceof Row)
			return (TT) serAcc;
		else if (serAcc instanceof scala.collection.immutable.Map)
		{
			// RankedPartition should be the only class that maps to a scala Map
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
			return (TT) asJava((scala.collection.Iterable<?>) serAcc);
		else		
			return (TT) serAcc;
	}

	private SparkUtils()
	{
	}
}
