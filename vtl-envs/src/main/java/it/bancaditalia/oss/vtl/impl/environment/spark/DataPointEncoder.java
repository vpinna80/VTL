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
import static java.util.Calendar.DAY_OF_MONTH;
import static java.util.Calendar.MONTH;
import static java.util.Calendar.YEAR;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDate;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.BooleanType$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType$;
import org.apache.spark.sql.types.DoubleType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UDTRegistration;

import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
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
import it.bancaditalia.oss.vtl.util.SerPredicate;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.SeqLike;

public class DataPointEncoder implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final DataStructureComponent<?, ?, ?>[] components;
	private final DataSetMetadata structure;
	private final StructType schema;
	private final Encoder<Row> rowEncoder;
	private final Encoder<Row> rowEncoderNoLineage;
	private final NullPointerException stack;
	
	public DataPointEncoder(Set<? extends DataStructureComponent<?, ?, ?>> dataStructure)
	{
		if (!UDTRegistration.exists(Lineage.class.getName()))
			UDTRegistration.register(Lineage.class.getName(), LineageSparkUDT.class.getName());
		
		try
		{
			throw new NullPointerException();
		}
		catch (NullPointerException e)
		{
			e.getStackTrace();
			stack = e;
		}

		components = dataStructure.toArray(new DataStructureComponent<?, ?, ?>[dataStructure.size()]);
		Arrays.sort(components, (dp1, dp2) -> dp1.getName().compareTo(dp2.getName()));
		structure = new DataStructureBuilder(dataStructure).build();
		List<StructField> fields = Arrays.stream(components)
			.parallel()
			.map(DataPointEncoder::componentToField)
			.collect(toCollection(ArrayList::new));

		rowEncoderNoLineage = RowEncoder.apply(new StructType(fields.toArray(new StructField[components.length])));

		fields.add(new StructField("$lineage$", LineageSparkUDT$.MODULE$, false, null));
		schema = new StructType(fields.toArray(new StructField[components.length]));
		rowEncoder = RowEncoder.apply(schema);
	}

	public Row encode(DataPoint dp)
	{
		try
		{
			return Arrays.stream(components)
				.parallel()
				.map(dp::get)
				.map(ScalarValue::get)
				.map(Object.class::cast)
				.map(DataPointEncoder::unwrapDate)
				.collect(collectingAndThen(collectingAndThen(collectingAndThen(collectingAndThen(
						toList(),
						l -> { l.add(dp.getLineage()); return l; }),
						JavaConverters::asScalaBuffer), 
						SeqLike::toSeq), 
						(Seq<Object> seq) -> Row.fromSeq(seq)));
		}
		catch (RuntimeException e)
		{
			stack.printStackTrace();
			throw new VTLNestedException("Exception while encoding datapoint " + dp + " with " + structure, e);
		}
	}

	public DataPoint decode(Row row)
	{
		return decode(row, 0);
	}

	public DataPoint decode(Row row, int startFrom)
	{
		try
		{
			Object lineageValue = row.get(startFrom + components.length);
			Lineage lineage = lineageValue instanceof byte[] ? LineageSparkUDT$.MODULE$.deserialize(lineageValue) : (Lineage) lineageValue;
			return IntStream.range(0, components.length)
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
	
	public MapFunction<Row, Row> wrap(MapFunction<DataPoint, DataPoint> mapper)
	{
		return wrapWithEncoder(wrapWithDecoder(mapper));
	}

	public <T> MapFunction<Row, T> wrapWithDecoder(MapFunction<DataPoint, T> mapper)
	{
		return row -> mapper.call(decode(row));
	}

	public <T> MapFunction<T, Row> wrapWithEncoder(MapFunction<T, DataPoint> mapper)
	{
		return dp -> encode(mapper.call(dp));
	}

	public FilterFunction<Row> wrapPredicate(SerPredicate<DataPoint> predicate)
	{
		return row -> predicate.test(decode(row));
	}

	static ScalarValue<?, ?, ?, ?> scalarFromColumnValue(Object object, DataStructureComponent<?, ?, ?> component)
	{
		ValueDomainSubset<?, ?> domain = component.getDomain();
		if (object == null)
			return NullValue.instanceFrom(component);
		if (INTEGERDS.equals(domain))
			return IntegerValue.of((Long) object);
		else if (NUMBERDS.equals(domain))
			return DoubleValue.of((Double) object);
		else if (STRINGDS.equals(domain))
			return StringValue.of((String) object);
		else if (BOOLEANDS.equals(domain))
			return BooleanValue.of((Boolean) object);
		else if (DATEDS.equals(domain))
		{
			Calendar date = Calendar.getInstance();
			date.setTime((Date) object);
			return DateValue.of(new DayHolder(LocalDate.of(date.get(YEAR), date.get(MONTH) + 1, date.get(DAY_OF_MONTH))));
		}
		else
			throw new UnsupportedOperationException();
	}

	private static StructField componentToField(DataStructureComponent<?, ?, ?> component)
	{
		DataType type;
		ValueDomainSubset<?, ?> domain = component.getDomain();
		if (INTEGERDS.equals(domain))
			type = LongType$.MODULE$;
		else if (NUMBERDS.equals(domain))
			type = DoubleType$.MODULE$;
		else if (STRINGDS.equals(domain))
			type = StringType$.MODULE$;
		else if (BOOLEANDS.equals(domain))
			type = BooleanType$.MODULE$;
		else if (DATEDS.equals(domain))
			type = DateType$.MODULE$;
		else
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

	private static Object unwrapDate(Object object)
	{
		return object instanceof DayHolder ? new Date(((DayHolder) object).toEpochMilli()) : object;
	}

	public Encoder<Row> getRowEncoderNoLineage()
	{
		return rowEncoderNoLineage;
	}

	public DataSetMetadata getStructure()
	{
		return structure;
	}
}
