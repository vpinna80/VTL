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
package it.bancaditalia.oss.vtl.impl.environment.spark.scalars;

import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.IntStream;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.UDTEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UserDefinedType;

import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;

public abstract class ScalarValueUDT<T extends ScalarValue<?, ?, ?, ?>> extends UserDefinedType<T>
{
	private static final long serialVersionUID = 1L;
	private static final Map<Class<?>, Entry<Integer, ScalarValueUDT<?>>> TAGS = new HashMap<>();
	private static final ScalarValueUDT<?>[] UDTS;
	
	static
	{
		UDTS = new ScalarValueUDT<?>[] {
			new BooleanValueUDT(),
			new StringValueUDT(),
			new IntegerValueUDT(),
			new DoubleValueUDT(),
			new BigDecimalValueUDT(),
			new DurationValueUDT(),
			new GenericTimeValueUDT(),
			new DateValueUDT(),
			new TimePeriodValueUDT()
		};
		
		IntStream.range(0, UDTS.length).forEach(i -> TAGS.put(UDTS[i].userClass(), new SimpleEntry<>(i, UDTS[i])));
	}

	public static final ScalarValueUDT<?> getUDTforTag(Integer tag)
	{
		return UDTS[tag];
	}
	
	public static final Entry<Integer, ScalarValueUDT<?>> getTagAndUDTForClass(Class<?> clazz)
	{
		return requireNonNull(TAGS.get(clazz));
	}

	private final StructType sqlType;
	private final String name = getClass().getSimpleName();
	
	public ScalarValueUDT(DataType sqlType)
	{
		this.sqlType = sqlType instanceof StructType ? (StructType) sqlType : createStructType(List.of(createStructField("value", sqlType, true)));
	}

	@Override
	public final T deserialize(Object datum)
	{
		InternalRow row = (InternalRow) datum;
		return row == null ? null : deserializeFrom(row, 0);
	}
	
	@Override
	public final InternalRow serialize(T obj)
	{
		if (obj instanceof NullValue)
			return null;
		else
		{
			InternalRow row = new GenericInternalRow(sqlType.size());
			serializeTo(obj, row, 0);
			return row;
		}
	}

	protected abstract T deserializeFrom(InternalRow row, int start);

	protected abstract void serializeTo(T obj, InternalRow row, int start);
	
	@SuppressWarnings("unchecked")
	public final Object serializeUnchecked(ScalarValue<?, ?, ?, ?> obj)
	{
		return serialize((T) obj);
	}
	
	@Override
	public final StructType sqlType()
	{
		return sqlType;
	}
	
	public final Integer getTag()
	{
		return TAGS.get(userClass()).getKey();
	}
	
	public final UDTEncoder<T> getEncoder()
	{
		@SuppressWarnings("unchecked")
		Class<? extends ScalarValueUDT<T>> udtClass = (Class<? extends ScalarValueUDT<T>>) this.getClass();
		return new UDTEncoder<>(this, udtClass);
	}
	
	@Override
	public String toString()
	{
		return name;
	}
}
