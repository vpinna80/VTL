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

import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.toDataPoint;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATEDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static org.apache.spark.sql.Encoders.BOOLEAN;
import static org.apache.spark.sql.Encoders.DOUBLE;
import static org.apache.spark.sql.Encoders.LOCALDATE;
import static org.apache.spark.sql.Encoders.LONG;
import static org.apache.spark.sql.Encoders.STRING;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.DayHolder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Attribute;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.ViralAttribute;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.util.SerFunction;

public class DataPointEncoder implements Serializable
{
	private static final long serialVersionUID = 1L;
	private static final Map<ValueDomainSubset<?, ?>, Encoder<?>> DOMAIN_ENCODERS = new HashMap<>();
	private static final Map<ValueDomainSubset<?, ?>, DataType> DOMAIN_DATATYPES = new HashMap<>();
	private static final Map<ValueDomainSubset<?, ?>, SerFunction<Object, ScalarValue<?, ?, ?, ?>>> DOMAIN_BUILDERS = new HashMap<>();
	
	static
	{
		DOMAIN_ENCODERS.put(BOOLEANDS, BOOLEAN());
		DOMAIN_ENCODERS.put(STRINGDS, STRING());
		DOMAIN_ENCODERS.put(INTEGERDS, LONG());
		DOMAIN_ENCODERS.put(NUMBERDS, DOUBLE());
		DOMAIN_ENCODERS.put(DATEDS, LOCALDATE());

		DOMAIN_DATATYPES.put(BOOLEANDS, BooleanType);
		DOMAIN_DATATYPES.put(STRINGDS, StringType);
		DOMAIN_DATATYPES.put(INTEGERDS, LongType);
		DOMAIN_DATATYPES.put(NUMBERDS, DoubleType);
		DOMAIN_DATATYPES.put(DATEDS, DateType);

		DOMAIN_BUILDERS.put(BOOLEANDS, v -> BooleanValue.of((Boolean) v));
		DOMAIN_BUILDERS.put(STRINGDS, v -> StringValue.of((String) v));
		DOMAIN_BUILDERS.put(INTEGERDS, v -> IntegerValue.of((Long) v));
		DOMAIN_BUILDERS.put(NUMBERDS, v -> DoubleValue.of((Double) v));
		DOMAIN_BUILDERS.put(DATEDS, v -> DateValue.of((LocalDate) v));
	}
	
	private final DataStructureComponent<?, ?, ?>[] components;
	private final DataSetMetadata structure;
	private final StructType schema;
	private final Encoder<Row> rowEncoder;
	private final Encoder<Row> rowEncoderNoLineage;
	
	public DataPointEncoder(Set<? extends DataStructureComponent<?, ?, ?>> dataStructure)
	{
		structure = dataStructure instanceof DataSetMetadata ? (DataSetMetadata) dataStructure : new DataStructureBuilder(dataStructure).build();
		components = structure.toArray(new DataStructureComponent<?, ?, ?>[structure.size()]);
		Arrays.sort(components, DataPointEncoder::sorter);
		List<StructField> fields = new ArrayList<>(createStructFromComponents(components));
		StructType schemaNoLineage = new StructType(fields.toArray(new StructField[components.length]));
		rowEncoderNoLineage = RowEncoder.apply(schemaNoLineage);
		fields.add(new StructField("$lineage$", LineageSparkUDT$.MODULE$, false, null));
		schema = new StructType(fields.toArray(new StructField[components.length + 1]));
		rowEncoder = RowEncoder.apply(schema);
	}

	static Set<DataStructureComponent<?, ?, ?>> createComponentsFromStruct(StructType schema)
	{
		Set<DataStructureComponent<?, ?, ?>> result = new HashSet<>();
		for (StructField field: schema.fields())
		{
			ValueDomainSubset<?, ?> domain = DOMAIN_DATATYPES.entrySet().stream()
					.filter(e -> field.dataType().equals(e.getValue()))
					.findAny()
					.map(Entry::getKey)
					.orElseThrow(() -> new UnsupportedOperationException("No VTL type corresponding to " + field.dataType()));
			Class<? extends ComponentRole> role;
			switch ((int) field.metadata().getLong("Role"))
			{
				case 1: role = Identifier.class; break;
				case 2: role = Measure.class; break;
				case 3: role = Attribute.class; break;
				case 4: role = ViralAttribute.class; break;
				default: throw new UnsupportedOperationException("No VTL role corresponding to metadata.");
			}
			result.add(DataStructureComponentImpl.of(field.name(), role, domain));
		}
		
		return result;
	}

	static List<StructField> createStructFromComponents(DataStructureComponent<?, ?, ?>[] components)
	{
		return structHelper(Arrays.stream(components), DataPointEncoder::componentToField);
	}

	static List<StructField> createStructFromComponents(Collection<? extends DataStructureComponent<?, ?, ?>> components)
	{
		return structHelper(components.stream(), DataPointEncoder::componentToField);
	}
	
	static List<String> getNamesFromComponents(Collection<? extends DataStructureComponent<?, ?, ?>> components)
	{
		return structHelper(components.stream(), DataStructureComponent::getName);
	}

	static List<Column> getColumnsFromComponents(Dataset<Row> dataFrame, Collection<? extends DataStructureComponent<?, ?, ?>> components)
	{
		return structHelper(components.stream(), c -> dataFrame.col(c.getName()));
	}

	private static <F> List<F> structHelper(Stream<? extends DataStructureComponent<?, ?, ?>> stream, SerFunction<? super DataStructureComponent<?, ?, ?>, F> mapper)
	{
		return stream
			.sorted(DataPointEncoder::sorter)
			.map(mapper)
			.collect(toList());
	}
	
	static int sorter(DataStructureComponent<?, ?, ?> c1, DataStructureComponent<?, ?, ?> c2)
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

	public Row encode(DataPoint dp)
	{
		try
		{
			return Arrays.stream(components)
				.map(dp::get)
				.map(ScalarValue::get)
				.map(Serializable.class::cast)
				.map(value -> value instanceof DayHolder ? ((DayHolder) value).getLocalDate() : value)
				.collect(collectingAndThen(collectingAndThen(
						toList(),
						l -> { l.add(dp.getLineage()); return l.toArray(new Object[l.size()]); }),
						GenericRow::new));
		}
		catch (RuntimeException e)
		{
			throw new VTLNestedException("Exception while encoding datapoint " + dp + " with " + structure, e);
		}
	}

	public DataPoint decode(Row row)
	{
		Object lineageValue;
		Lineage lineage;
		try
		{
			lineageValue = row.get(components.length);
		}
		catch (RuntimeException e)
		{
			throw new VTLNestedException("Exception while decoding row " + row + " with " + Arrays.toString(components), e);
		}
		try
		{
			lineage = lineageValue instanceof byte[] ? LineageSparkUDT$.MODULE$.deserialize(lineageValue) : (Lineage) lineageValue;
		}
		catch (RuntimeException e)
		{
			throw new VTLNestedException("Exception while decoding row " + row + " with " + Arrays.toString(components), e);
		}
		try
		{
			return IntStream.range(0, components.length)
				.mapToObj(i -> new SimpleEntry<>(components[i], scalarFromColumnValue(row.get(i), components[i])))
				.collect(toDataPoint(lineage, getStructure()));
		}
		catch (RuntimeException e)
		{
			throw new VTLNestedException("Exception while decoding row " + row + " with " + Arrays.toString(components), e);
		}
	}

	public StructType getSchema()
	{
		return schema;
	}
	
	public Encoder<Row> getRowEncoder()
	{
		return rowEncoder;
	}

	static ScalarValue<?, ?, ?, ?> scalarFromColumnValue(Object serialized, DataStructureComponent<?, ?, ?> component)
	{
		SerFunction<Object, ScalarValue<?, ?, ?, ?>> builder = DOMAIN_BUILDERS.get(component.getDomain());
		if (builder != null)
			return builder.apply(serialized);
		else
			throw new UnsupportedOperationException("Unsupported decoding of domain " + component.getDomain());
	}

	static StructField componentToField(DataStructureComponent<?, ?, ?> component)
	{
		DataType type = getDataTypeForComponent(component);
		Metadata metadata = createMetadataForComponent(component);
		
		return new StructField(component.getName(), type, component.is(NonIdentifier.class), metadata);
	}

	static Metadata createMetadataForComponent(DataStructureComponent<?, ?, ?> component)
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
		return metadataBuilder.build();
	}

	public Encoder<Row> getRowEncoderNoLineage()
	{
		return rowEncoderNoLineage;
	}

	public DataSetMetadata getStructure()
	{
		return structure;
	}
	
	@SuppressWarnings("unchecked")
	static Encoder<? extends Serializable> getEncoderForComponent(DataStructureComponent<?, ?, ?> component)
	{
		Encoder<?> encoder = DOMAIN_ENCODERS.get(component.getDomain());
		if (encoder != null)
			return (Encoder<? extends Serializable>) encoder;
		else
			throw new UnsupportedOperationException("Unsupported serialization for domain " + component.getDomain());
	}
	
	static DataType getDataTypeForComponent(DataStructureComponent<?, ?, ?> component)
	{
		DataType type = DOMAIN_DATATYPES.get(component.getDomain());
		if (type != null)
			return type;
		else
			throw new UnsupportedOperationException("Unsupported datatype for domain " + component.getDomain());
	}
}
