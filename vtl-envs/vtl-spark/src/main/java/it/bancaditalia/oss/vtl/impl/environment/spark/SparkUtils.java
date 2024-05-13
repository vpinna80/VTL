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
import static it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl.createNumberValue;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.Encoders.BOOLEAN;
import static org.apache.spark.sql.Encoders.DOUBLE;
import static org.apache.spark.sql.Encoders.LOCALDATE;
import static org.apache.spark.sql.Encoders.LONG;
import static org.apache.spark.sql.Encoders.STRING;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createDecimalType;

import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.Component.ViralAttribute;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.SerFunction;

public class SparkUtils
{
	private static final Map<ValueDomainSubset<?, ?>, Encoder<? extends Serializable>> DOMAIN_ENCODERS = new ConcurrentHashMap<>();
	private static final Map<ValueDomainSubset<?, ?>, DataType> DOMAIN_DATATYPES = new ConcurrentHashMap<>();
	private static final Map<ValueDomainSubset<?, ?>, SerFunction<Object, ScalarValue<?, ?, ?, ?>>> DOMAIN_BUILDERS = new ConcurrentHashMap<>();

	static
	{
		DOMAIN_ENCODERS.put(BOOLEANDS, BOOLEAN());
		DOMAIN_ENCODERS.put(STRINGDS, STRING());
		DOMAIN_ENCODERS.put(INTEGERDS, LONG());
		DOMAIN_ENCODERS.put(NUMBERDS, DOUBLE());
		DOMAIN_ENCODERS.put(TIMEDS, LOCALDATE());
		DOMAIN_ENCODERS.put(DATEDS, LOCALDATE());

		DOMAIN_DATATYPES.put(BOOLEANDS, BooleanType);
		DOMAIN_DATATYPES.put(STRINGDS, StringType);
		DOMAIN_DATATYPES.put(INTEGERDS, LongType);
		
		if (isUseBigDecimal())
			DOMAIN_DATATYPES.put(NUMBERDS, createDecimalType());
		else
			DOMAIN_DATATYPES.put(NUMBERDS, DoubleType);
		DOMAIN_DATATYPES.put(TIMEDS, DateType);
		DOMAIN_DATATYPES.put(DATEDS, DateType);

		DOMAIN_BUILDERS.put(BOOLEANDS, v -> BooleanValue.of((Boolean) v));
		DOMAIN_BUILDERS.put(STRINGDS, v -> StringValue.of((String) v));
		DOMAIN_BUILDERS.put(INTEGERDS, v -> IntegerValue.of((Long) v));
		DOMAIN_BUILDERS.put(NUMBERDS, v -> createNumberValue((Number) v));
		DOMAIN_BUILDERS.put(TIMEDS, v -> DateValue.of((LocalDate) v));
		DOMAIN_BUILDERS.put(DATEDS, v -> DateValue.of((LocalDate) v));
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
		
		return repo.getVariable(field.name()).as(role);
	}

	public static ScalarValue<?, ?, ?, ?> getScalarFor(DataStructureComponent<?, ?, ?> component, Object serialized)
	{
		SerFunction<Object, ScalarValue<?, ?, ?, ?>> builder = null;
		ValueDomainSubset<?, ?> domain;
		for (domain = component.getVariable().getDomain(); domain != null && builder == null; domain = (ValueDomainSubset<?, ?>) domain.getParentDomain()) 
			builder = DOMAIN_BUILDERS.get(domain);
		
		if (builder == null)
			throw new UnsupportedOperationException("Unsupported decoding of domain " + component.getVariable().getDomain());

		domain = component.getVariable().getDomain();
		DOMAIN_BUILDERS.putIfAbsent(domain, builder);

		if (serialized instanceof Date)
			serialized = (((Date) serialized).toLocalDate());
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
		
		return metadataBuilder.putString("Domain", component.getVariable().getDomain().getName()).build();
	}

	public static StructField getFieldFor(DataStructureComponent<?, ?, ?> component)
	{
		DataType type = getDataTypeFor(component);
		Metadata metadata = getMetadataFor(component);
		
		return new StructField(component.getVariable().getName(), type, component.is(NonIdentifier.class), metadata);
	}

	public static Encoder<? extends Serializable> getEncoderFor(DataStructureComponent<?, ?, ?> component)
	{
		return requireNonNull(DOMAIN_ENCODERS.get(component.getVariable().getDomain()), "Unsupported serialization for domain " + component.getVariable().getDomain());
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

	public static List<StructField> createStructFromComponents(Collection<? extends DataStructureComponent<?, ?, ?>> components)
	{
		return structHelper(components.stream(), SparkUtils::getFieldFor);
	}

	public static List<String> getNamesFromComponents(Collection<? extends DataStructureComponent<?, ?, ?>> components)
	{
		return structHelper(components.stream(), c -> c.getVariable().getName());
	}

	public static List<Column> getColumnsFromComponents(Collection<? extends DataStructureComponent<?, ?, ?>> components)
	{
		return structHelper(components.stream(), c -> col(c.getVariable().getName()));
	}

	public static <F> List<F> structHelper(Stream<? extends DataStructureComponent<?, ?, ?>> stream, SerFunction<? super DataStructureComponent<?, ?, ?>, F> mapper)
	{
		return stream
			.sorted(DataStructureComponent::byNameAndRole)
			.map(mapper)
			.collect(toList());
	}

	private SparkUtils()
	{
	}
}
