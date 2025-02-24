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

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.UDTEncoder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UserDefinedType;

import it.bancaditalia.oss.vtl.model.data.ScalarValue;

public abstract class ScalarValueUDT<T extends ScalarValue<?, ?, ?, ?>> extends UserDefinedType<T>
{
	private static final long serialVersionUID = 1L;
	private static final Map<Class<?>, Entry<Integer, ScalarValueUDT<?>>> TAGS = new HashMap<>();
	private static final ScalarValueUDT<?>[] UDTS = new ScalarValueUDT<?>[] {
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
	
	static
	{
		for (int i = 0; i < UDTS.length; i++)
			TAGS.put(UDTS[i].userClass(), new SimpleEntry<>(i, UDTS[i]));
	}

	public static final ScalarValueUDT<?> getUDTforTag(Integer tag)
	{
		return UDTS[tag];
	}
	
	public static final Entry<Integer, ScalarValueUDT<?>> getUDTForClass(Class<?> clazz)
	{
		return requireNonNull(TAGS.get(clazz));
	}

	private final StructType sqlType;
	
	public ScalarValueUDT(StructType sqlType)
	{
		this.sqlType = sqlType;
	}

	@SuppressWarnings("unchecked")
	public final Object serializeScalar(ScalarValue<?, ?, ?, ?> obj)
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
}
