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

import static org.apache.spark.sql.types.DataTypes.createDecimalType;

import java.math.BigDecimal;

import org.apache.spark.sql.types.DecimalType;

import it.bancaditalia.oss.vtl.impl.types.data.BigDecimalValue;

public class BigDecimalValueUDT extends SingleFieldScalarValueUDT<BigDecimalValue<?>>
{
	private static final long serialVersionUID = 1L;

	private static final DecimalType DECIMAL_TYPE = createDecimalType();

	public BigDecimalValueUDT()
	{
		super(DECIMAL_TYPE);
	}
	
	@Override
	public BigDecimalValue<?> deserializeInternal(Object datum)
	{
		return (BigDecimalValue<?>) BigDecimalValue.of((BigDecimal) datum);
	}

	@Override
	public Object serializeInternal(BigDecimalValue<?> obj)
	{
		return obj.get();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<BigDecimalValue<?>> userClass()
	{
		return (Class<BigDecimalValue<?>>) (Class<?>) BigDecimalValue.class;
	}
}
