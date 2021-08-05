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
import static it.bancaditalia.oss.vtl.model.data.DataStructureComponent.byName;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataType;
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
import it.bancaditalia.oss.vtl.util.Utils;

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
	private final NullPointerException stack;
	
	public DataPointEncoder(Set<? extends DataStructureComponent<?, ?, ?>> dataStructure)
	{
		try
		{
			throw new NullPointerException();
		}
		catch (NullPointerException e)
		{
			e.getStackTrace();
			stack = e;
		}

		structure = new DataStructureBuilder(dataStructure).build();

		components = dataStructure.toArray(new DataStructureComponent<?, ?, ?>[dataStructure.size()]);
		Arrays.sort(components, DataStructureComponent.byName());
		List<StructField> fields = new ArrayList<>(createStructFromComponents(components));
		StructType schemaNoLineage = new StructType(fields.toArray(new StructField[components.length]));
		rowEncoderNoLineage = RowEncoder.apply(schemaNoLineage);
		fields.add(new StructField("$lineage$", LineageSparkUDT$.MODULE$, false, null));
		schema = new StructType(fields.toArray(new StructField[components.length + 1]));
		rowEncoder = RowEncoder.apply(schema);
	}

	static List<StructField> createStructFromComponents(DataStructureComponent<?, ?, ?>[] components)
	{
		return Arrays.stream(components)
			.sorted(byName())
			.map(DataPointEncoder::componentToField)
			.collect(toCollection(ArrayList::new));
	}

	static List<StructField> createStructFromComponents(Collection<? extends DataStructureComponent<?, ?, ?>> components)
	{
		return createStructFromComponents(components.toArray(new DataStructureComponent<?, ?, ?>[components.size()]));
	}

	public Row encode(DataPoint dp)
	{
		try
		{
			return Arrays.stream(components)
				.sorted(byName())
				.map(dp::get)
				.map(ScalarValue::get)
				.map(Object.class::cast)
				.map(value -> value instanceof DayHolder ? ((DayHolder) value).getLocalDate() : value)
				.collect(collectingAndThen(collectingAndThen(
						toList(),
						l -> { l.add(dp.getLineage()); return l.toArray(new Object[l.size()]); }),
						GenericRow::new));
		}
		catch (RuntimeException e)
		{
			stack.printStackTrace();
			throw new VTLNestedException("Exception while encoding datapoint " + dp + " with " + structure, e);
		}
	}

	public DataPoint decode(Object[] row)
	{
		return decode(row, 0);
	}

	public DataPoint decode(Object[] row, int startFrom)
	{
		try
		{
			Object lineageValue = row[startFrom + components.length];
			Lineage lineage = lineageValue instanceof byte[] ? LineageSparkUDT$.MODULE$.deserialize(lineageValue) : (Lineage) lineageValue;
			return IntStream.range(0, components.length)
				.parallel()
				.mapToObj(i -> new SimpleEntry<>(components[i], scalarFromColumnValue(row[startFrom + i], components[i])))
				.collect(toDataPoint(lineage, getStructure()));
		}
		catch (RuntimeException e)
		{
			stack.printStackTrace();
			throw new VTLNestedException("Exception while decoding row " + row + " with " + structure, e);
		}
	}

	public DataPoint decode(Row row)
	{
		return decode(row, 0);
	}

	public SerFunction<Row, DataPoint> decodeFrom(int startFrom)
	{
		return new SerFunction<Row, DataPoint>()
		{
			private static final long serialVersionUID = 1L;

			@Override
			public DataPoint apply(Row row)
			{
				return decode(row, startFrom);
			}
		};
	}

	public DataPoint decode(Row row, int startFrom)
	{
		try
		{
			Object lineageValue = row.get(startFrom + components.length);
			Lineage lineage = lineageValue instanceof byte[] ? LineageSparkUDT$.MODULE$.deserialize(lineageValue) : (Lineage) lineageValue;
			IntStream range = IntStream.range(0, components.length);
			return (Utils.SEQUENTIAL ? range : range.parallel())
				.parallel()
				.mapToObj(i -> new SimpleEntry<>(components[i], scalarFromColumnValue(row.get(startFrom + i), components[i])))
				.collect(toDataPoint(lineage, getStructure()));
		}
		catch (RuntimeException e)
		{
			stack.printStackTrace();
			throw new VTLNestedException("Exception while decoding row " + row + " with " + structure, e);
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
			try
			{
				return builder.apply(serialized);
			}
			catch (ClassCastException e)
			{
				throw e;
			}
		else
			throw new UnsupportedOperationException();
	}

	static StructField componentToField(DataStructureComponent<?, ?, ?> component)
	{
		DataType type = DOMAIN_DATATYPES.get(component.getDomain());
		if (type == null)
			throw new UnsupportedOperationException("Spark type not supported for " + component.getDomain());

		MetadataBuilder metadataBuilder = new MetadataBuilder();
		if (component.getRole() == Identifier.class)
			metadataBuilder.putLong("Role", 1);
		else if (component.getRole() == Measure.class)
			metadataBuilder.putLong("Role", 2);
		else if (component.getRole() == Attribute.class)
			metadataBuilder.putLong("Role", 3);
		else if (component.getRole() == ViralAttribute.class)
			metadataBuilder.putLong("Role", 4);
		
		return new StructField(component.getName(), type, component.is(NonIdentifier.class), metadataBuilder.build());
	}

	public Encoder<Row> getRowEncoderNoLineage()
	{
		return rowEncoderNoLineage;
	}

	public DataSetMetadata getStructure()
	{
		return structure;
	}
	
	public static Encoder<?> getEncoderForComponent(DataStructureComponent<?, ?, ?> component)
	{
		Encoder<?> encoder = DOMAIN_ENCODERS.get(component.getDomain());
		if (encoder != null)
			return encoder;
		else
			throw new UnsupportedOperationException(component.getDomain().toString());
	}

	
	public static DataType getDataTypeForComponent(DataStructureComponent<?, ?, ?> component)
	{
		DataType type = DOMAIN_DATATYPES.get(component.getDomain());
		if (type != null)
			return type;
		else
			throw new UnsupportedOperationException(component.getDomain().toString());
	}
}
