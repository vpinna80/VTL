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

import java.math.BigDecimal;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.UserDefinedType;

import it.bancaditalia.oss.vtl.impl.types.data.BigDecimalValue;

public class BigDecimalValueUDT extends UserDefinedType<BigDecimalValue<?>>
{
	private static final long serialVersionUID = 1L;

	@Override
	public BigDecimalValue<?> deserialize(Object datum)
	{
		return (BigDecimalValue<?>) BigDecimalValue.of((BigDecimal) datum);
	}

	@Override
	public Object serialize(BigDecimalValue<?> obj)
	{
		return obj.get();
	}

	@Override
	public DataType sqlType()
	{
		return DecimalType.USER_DEFAULT();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<BigDecimalValue<?>> userClass()
	{
		return (Class<BigDecimalValue<?>>) (Class<? extends BigDecimalValue<?>>) BigDecimalValue.class;
	}
	
	@Override
	public String toString()
	{
		return "BigDecimalValue";
	}
}
