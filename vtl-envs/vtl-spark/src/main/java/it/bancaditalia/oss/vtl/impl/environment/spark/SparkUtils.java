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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Attribute;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.ViralAttribute;
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
	private static final MetadataRepository METAREPO = ConfigurationManager.getDefault().getMetadataRepository();

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
		DOMAIN_DATATYPES.put(NUMBERDS, DoubleType);
		DOMAIN_DATATYPES.put(TIMEDS, DateType);
		DOMAIN_DATATYPES.put(DATEDS, DateType);

		DOMAIN_BUILDERS.put(BOOLEANDS, v -> BooleanValue.of((Boolean) v));
		DOMAIN_BUILDERS.put(STRINGDS, v -> StringValue.of((String) v));
		DOMAIN_BUILDERS.put(INTEGERDS, v -> IntegerValue.of((Long) v));
		DOMAIN_BUILDERS.put(NUMBERDS, v -> DoubleValue.of((Double) v));
		DOMAIN_BUILDERS.put(TIMEDS, v -> DateValue.of((LocalDate) v));
		DOMAIN_BUILDERS.put(DATEDS, v -> DateValue.of((LocalDate) v));
	}

	public static Set<DataStructureComponent<?, ?, ?>> getComponentsFromStruct(StructType schema)
	{
		Set<DataStructureComponent<?, ?, ?>> result = new HashSet<>();
		for (StructField field: schema.fields())
			result.add(getComponentFor(field));
		
		return result;
	}

	private static DataStructureComponent<? extends ComponentRole, ?, ?> getComponentFor(StructField field)
	{
		Class<? extends ComponentRole> role;
		switch ((int) field.metadata().getLong("Role"))
		{
			case 1: role = Identifier.class; break;
			case 2: role = Measure.class; break;
			case 3: role = Attribute.class; break;
			case 4: role = ViralAttribute.class; break;
			default: throw new UnsupportedOperationException("No VTL role corresponding to metadata.");
		}
		
		ValueDomainSubset<?, ?> domain = METAREPO.getDomain(field.metadata().getString("Domain"));
		
		return DataStructureComponentImpl.of(field.name(), role, domain);
	}

	public static ScalarValue<?, ?, ?, ?> getScalarFor(Object serialized, DataStructureComponent<?, ?, ?> component)
	{
		SerFunction<Object, ScalarValue<?, ?, ?, ?>> builder = null;
		ValueDomainSubset<?, ?> domain;
		for (domain = component.getDomain(); domain != null && builder == null; domain = (ValueDomainSubset<?, ?>) domain.getParentDomain()) 
			builder = DOMAIN_BUILDERS.get(domain);
		
		if (builder == null)
			throw new UnsupportedOperationException("Unsupported decoding of domain " + component.getDomain());

		domain = component.getDomain();
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
		
		return metadataBuilder.putString("Domain", component.getDomain().getName()).build();
	}

	public static StructField getFieldFor(DataStructureComponent<?, ?, ?> component)
	{
		DataType type = getDataTypeFor(component);
		Metadata metadata = getMetadataFor(component);
		
		return new StructField(component.getName(), type, component.is(NonIdentifier.class), metadata);
	}

	public static Encoder<? extends Serializable> getEncoderFor(DataStructureComponent<?, ?, ?> component)
	{
		return requireNonNull(DOMAIN_ENCODERS.get(component.getDomain()), "Unsupported serialization for domain " + component.getDomain());
	}

	public static DataType getDataTypeFor(DataStructureComponent<?, ?, ?> component)
	{
		ValueDomainSubset<?, ?> domain = component.getDomain();
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
		return structHelper(components.stream(), DataStructureComponent::getName);
	}

	public static List<Column> getColumnsFromComponents(Collection<? extends DataStructureComponent<?, ?, ?>> components)
	{
		return structHelper(components.stream(), c -> col(c.getName()));
	}

	public static <F> List<F> structHelper(Stream<? extends DataStructureComponent<?, ?, ?>> stream, SerFunction<? super DataStructureComponent<?, ?, ?>, F> mapper)
	{
		return stream
			.sorted(SparkUtils::sorter)
			.map(mapper)
			.collect(toList());
	}

	public static int sorter(DataStructureComponent<?, ?, ?> c1, DataStructureComponent<?, ?, ?> c2)
	{
		if (c1.is(Attribute.class) && !c2.is(Attribute.class))
			return 1;
		else if (c1.is(Identifier.class) && !c2.is(Identifier.class))
			return -1;
		else if (c1.is(Measure.class) && c2.is(Identifier.class))
			return 1;
		else if (c1.is(Measure.class) && c2.is(Attribute.class))
			return -1;
	
		String n1 = c1.getName(), n2 = c2.getName();
		Pattern pattern = Pattern.compile("^(.+?)(\\d+)$");
		Matcher m1 = pattern.matcher(n1), m2 = pattern.matcher(n2);
		if (m1.find() && m2.find() && m1.group(1).equals(m2.group(1)))
			return Integer.compare(Integer.parseInt(m1.group(2)), Integer.parseInt(m2.group(2)));
		else
			return n1.compareTo(n2);
	}

	private SparkUtils()
	{
	}
}
