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

import static org.apache.spark.sql.types.DataTypes.LongType;

import org.apache.spark.sql.catalyst.InternalRow;

import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;

public class IntegerValueUDT extends ScalarValueUDT<IntegerValue<?, ?>>
{
	private static final long serialVersionUID = 1L;

	public IntegerValueUDT()
	{
		super(LongType);
	}
	
	@Override
	protected IntegerValue<?, ?> deserializeFrom(InternalRow row, int start)
	{
		return (IntegerValue<?, ?>) IntegerValue.of(row.getLong(start));
	}
	
	@Override
	protected void serializeTo(IntegerValue<?, ?> obj, InternalRow row, int start)
	{
		row.setLong(start, obj.get());
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<IntegerValue<?, ?>> userClass()
	{
		return (Class<IntegerValue<?, ?>>) (Class<?>) IntegerValue.class;
	}
}
