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

import static org.apache.spark.sql.types.DataTypes.StringType;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.UserDefinedType;
import org.apache.spark.unsafe.types.UTF8String;

import it.bancaditalia.oss.vtl.impl.types.data.StringValue;

public class StringValueUDT extends UserDefinedType<StringValue<?, ?>>
{
	private static final long serialVersionUID = 1L;

	@Override
	public StringValue<?, ?> deserialize(Object datum)
	{
		return (StringValue<?, ?>) StringValue.of(((UTF8String) datum).toString());
	}

	@Override
	public Object serialize(StringValue<?, ?> obj)
	{
		return UTF8String.fromString(obj.get());
	}

	@Override
	public DataType sqlType()
	{
		return StringType;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<StringValue<?, ?>> userClass()
	{
		return (Class<StringValue<?, ?>>) (Class<? extends StringValue<?, ?>>) StringValue.class;
	}
	
	@Override
	public String toString()
	{
		return "StringValue";
	}
}
