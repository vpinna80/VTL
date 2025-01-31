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

import static org.apache.spark.sql.types.DataTypes.DoubleType;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.UserDefinedType;

import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;

public class DoubleValueUDT extends UserDefinedType<DoubleValue<?>>
{
	private static final long serialVersionUID = 1L;

	@Override
	public DoubleValue<?> deserialize(Object datum)
	{
		return (DoubleValue<?>) DoubleValue.of((Double) datum, Domains.NUMBERDS);
	}

	@Override
	public Object serialize(DoubleValue<?> obj)
	{
		return obj.get();
	}

	@Override
	public DataType sqlType()
	{
		return DoubleType;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<DoubleValue<?>> userClass()
	{
		return (Class<DoubleValue<?>>) (Class<? extends DoubleValue<?>>) DoubleValue.class;
	}
	
	@Override
	public String toString()
	{
		return "DoubleValue";
	}
}
