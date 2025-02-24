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
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static org.apache.spark.sql.functions.col;
import static scala.jdk.javaapi.CollectionConverters.asScala;

import java.io.Serializable;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.JavaTypeInference;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.EncoderField;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.ProductEncoder;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.UDTEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.environment.spark.scalars.BigDecimalValueUDT;
import it.bancaditalia.oss.vtl.impl.environment.spark.scalars.BooleanValueUDT;
import it.bancaditalia.oss.vtl.impl.environment.spark.scalars.DateValueUDT;
import it.bancaditalia.oss.vtl.impl.environment.spark.scalars.DoubleValueUDT;
import it.bancaditalia.oss.vtl.impl.environment.spark.scalars.DurationValueUDT;
import it.bancaditalia.oss.vtl.impl.environment.spark.scalars.GenericTimeValueUDT;
import it.bancaditalia.oss.vtl.impl.environment.spark.scalars.IntegerValueUDT;
import it.bancaditalia.oss.vtl.impl.environment.spark.scalars.ScalarValueUDT;
import it.bancaditalia.oss.vtl.impl.environment.spark.scalars.StringValueUDT;
import it.bancaditalia.oss.vtl.impl.environment.spark.scalars.TimePeriodValueUDT;
import it.bancaditalia.oss.vtl.impl.environment.spark.udts.PartitionToRankUDT;
import it.bancaditalia.oss.vtl.impl.environment.spark.udts.RankedPartitionUDT;
import it.bancaditalia.oss.vtl.impl.environment.spark.udts.TimeWithFreqUDT;
import it.bancaditalia.oss.vtl.impl.types.data.BaseScalarValue;
import it.bancaditalia.oss.vtl.impl.types.data.BigDecimalValue;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.DurationValue;
import it.bancaditalia.oss.vtl.impl.types.data.GenericTimeValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.TimeWithFreq;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.impl.types.operators.PartitionToRank;
import it.bancaditalia.oss.vtl.impl.types.window.RankedPartition;
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
import it.bancaditalia.oss.vtl.util.Holder;
import it.bancaditalia.oss.vtl.util.SerDoubleSumAvgCount;
import it.bancaditalia.oss.vtl.util.SerFunction;
import scala.Option;
import scala.collection.immutable.Seq;
import scala.reflect.ClassTag;

public class SparkUtils
{
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(SparkUtils.class);
	
	@SuppressWarnings("rawtypes")
	private static final Map<Class<? extends ScalarValue>, AgnosticEncoder<?>> SCALAR_ENCODERS = new ConcurrentHashMap<>();
	private static final Map<ValueDomainSubset<?, ?>, ScalarValueUDT<?>> DOMAIN_DATATYPES = new ConcurrentHashMap<>();
	
	static
	{
		SCALAR_ENCODERS.put(BooleanValue.class, new BooleanValueUDT().getEncoder());
		SCALAR_ENCODERS.put(StringValue.class, new StringValueUDT().getEncoder());
		SCALAR_ENCODERS.put(IntegerValue.class, new IntegerValueUDT().getEncoder());
		SCALAR_ENCODERS.put(DoubleValue.class, new DoubleValueUDT().getEncoder());
		SCALAR_ENCODERS.put(BigDecimalValue.class, new BigDecimalValueUDT().getEncoder());
		SCALAR_ENCODERS.put(DurationValue.class, new DurationValueUDT().getEncoder());
		SCALAR_ENCODERS.put(GenericTimeValue.class, new GenericTimeValueUDT().getEncoder());
		SCALAR_ENCODERS.put(DateValue.class, new DateValueUDT().getEncoder());
		SCALAR_ENCODERS.put(TimePeriodValue.class, new TimePeriodValueUDT().getEncoder());

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
		
		return repo.getVariable(VTLAliasImpl.of(field.name())).orElseThrow(() -> new NullPointerException("Variable " + field.name() + " is not defined")).as(role);
	}

	public static MapFunction<Row, Row> parseCSVStrings(DataSetMetadata structure, Lineage lineage, DataPointEncoder encoder)
	{
		DataStructureComponent<?, ?, ?>[] comps = encoder.components;
		ValueDomainSubset<?,?>[] domains = encoder.domains;
		String[] names = new String[comps.length];
		for (int i = 0; i < names.length; i++)
			names[i] = comps[i].getVariable().getAlias().getName();
		
		return srcRow -> {
			int size = srcRow.size() + 1;
			Serializable[] newCols = new Serializable[size];
			for (int i = 0; i < size - 1; i++)
			{
				newCols[i] = mapValue(domains[i], srcRow.getAs(names[i]), null);
				if (((ScalarValue<?, ?, ?, ?>) newCols[i]).isNull())
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

	public static DataType getDataTypeFor(DataStructureComponent<?, ?, ?> component)
	{
		ValueDomainSubset<?, ?> domain = component.getVariable().getDomain();
		while (!DOMAIN_DATATYPES.containsKey(domain))
			domain = (ValueDomainSubset<?, ?>) domain.getParentDomain();
		if (DOMAIN_DATATYPES.containsKey(domain))
			return DOMAIN_DATATYPES.get(domain);
		else
			throw new UnsupportedOperationException("Domain " + domain + " is not supported on Spark.");
	}

	public static EncoderField getEncoderFieldFor(DataStructureComponent<?, ?, ?> comp)
	{
		ScalarValueUDT<?> scalarValueUDT = DOMAIN_DATATYPES.get(comp.getVariable().getDomain());
		return new EncoderField(comp.getVariable().getAlias().getName(), scalarValueUDT.getEncoder(), 
				!comp.is(Identifier.class), getMetadataFor(comp), Option.empty(), Option.empty());
	}

	public static List<EncoderField> createFieldFromComponents(Collection<? extends DataStructureComponent<?, ?, ?>> components)
	{
		return structHelper(components.stream(), SparkUtils::getEncoderFieldFor);
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

	/**
	 * Infers an encoder for a given instance to be used in window functions.
	 * If the instance is a ScalarValue, the encoder is from its domain.
	 * NOTE: The third parameter is optional and used for a specific case.
	 * 
	 * @param <T>
	 * @param instance
	 * @param structure
	 * @return
	 * @throws SecurityException 
	 * @throws IllegalArgumentException 
	 */
	public static <T> AgnosticEncoder<T> getEncoderFor(Serializable instance, DataSetMetadata structure)
	{
		AgnosticEncoder<?> resultEncoder; 
		
		Class<? extends Serializable> clazz = instance.getClass();
		if (instance instanceof BaseScalarValue)
		{
			if (((ScalarValue<?, ?, ?, ?>) instance).isNull())
			{
				ValueDomainSubset<?, ?> domain = ((ScalarValue<?, ?, ?, ?>) instance).getMetadata().getDomain();
				ScalarValueUDT<?> type = DOMAIN_DATATYPES.get(domain);
				if (type == null)
					throw new UnsupportedOperationException(domain.toString());
				resultEncoder = type.getEncoder();
			}
			else
			{
				resultEncoder = SCALAR_ENCODERS.get(clazz);
				if (resultEncoder == null)
					throw new UnsupportedOperationException(clazz.getSimpleName());
			}
		}
		else if (clazz == PartitionToRank.class)
		{
			resultEncoder = new UDTEncoder<>(new PartitionToRankUDT(structure), PartitionToRankUDT.class);
		}
		else if (clazz == RankedPartition.class)
		{
			resultEncoder = new UDTEncoder<>(new RankedPartitionUDT(structure), RankedPartitionUDT.class);
		}
		else if (clazz == SerDoubleSumAvgCount.class)
		{
			resultEncoder = JavaTypeInference.encoderFor(SerDoubleSumAvgCount.class);
		}
		else if (clazz == Holder.class)
		{
			try
			{
				List<EncoderField> fieldEncoders = new ArrayList<>();
				Class<?> heldClass = (Class<?>) clazz.getField("repr").get(instance);
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
		else if (clazz == TimeWithFreq.class)
		{
			resultEncoder = new UDTEncoder<>(new TimeWithFreqUDT(), TimeWithFreqUDT.class);
		}
		else 
			throw new IllegalStateException(clazz.getName());
		
		@SuppressWarnings("unchecked")
		AgnosticEncoder<T> encoder = (AgnosticEncoder<T>) resultEncoder;
		return encoder;
	}

	private SparkUtils()
	{
	}
}
