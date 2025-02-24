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

import static org.apache.spark.sql.types.DataTypes.IntegerType;

import java.time.LocalDate;

import it.bancaditalia.oss.vtl.impl.types.data.DateValue;

public class DateValueUDT extends SingleFieldScalarValueUDT<DateValue<?>>
{
	private static final long serialVersionUID = 1L;

	public DateValueUDT()
	{
		super(IntegerType);
	}

	@Override
	public DateValue<?> deserializeInternal(Object datum)
	{
		return (DateValue<?>) DateValue.of(LocalDate.ofEpochDay((Integer) datum));
	}

	@Override
	public Object serializeInternal(DateValue<?> obj)
	{
		return (int) obj.get().toEpochDay();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<DateValue<?>> userClass()
	{
		return (Class<DateValue<?>>) (Class<?>) DateValue.class;
	}
}
