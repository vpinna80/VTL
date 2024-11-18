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

import static org.apache.spark.sql.types.DataTypes.IntegerType;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.UserDefinedType;

import it.bancaditalia.oss.vtl.impl.types.data.Frequency;

public class FrequencySparkUDT extends UserDefinedType<Frequency>
{
	private static final long serialVersionUID = 1L;
	private static final Frequency[] VALUES = Frequency.values();
	
	@Override
	public Frequency deserialize(Object datum)
	{
		return datum != null ? VALUES[(Integer) datum] : null;
	}

	@Override
	public Integer serialize(Frequency frequency)
	{
		return frequency != null ? Integer.valueOf(frequency.ordinal()) : null;
	}

	@Override
	public DataType sqlType()
	{
		return IntegerType;
	}

	@Override
	public Class<Frequency> userClass()
	{
		return Frequency.class;
	}
}
