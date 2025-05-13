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

import static java.math.RoundingMode.UNNECESSARY;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.createDecimalType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

import java.math.BigDecimal;
import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;

import it.bancaditalia.oss.vtl.impl.types.data.BigDecimalValue;

public class BigDecimalValueUDT extends ScalarValueUDT<BigDecimalValue<?>>
{
	private static final long serialVersionUID = 1L;

	private static final StructType SQL_TYPE = createStructType(List.of(
			createStructField("scale", IntegerType, false),
			createStructField("value", createDecimalType(38, 19), false)
		));

	public BigDecimalValueUDT()
	{
		super(SQL_TYPE);
	}
	
	@Override
	protected BigDecimalValue<?> deserializeFrom(InternalRow row, int start)
	{
	    int scale = row.getInt(start);
		BigDecimal bigDecimal = (row.getDecimal(start + 1, 38, 19)).toJavaBigDecimal().setScale(scale);
		return (BigDecimalValue<?>) BigDecimalValue.of(bigDecimal);
	}

	@Override
	protected void serializeTo(BigDecimalValue<?> obj, InternalRow row, int start) 
	{
		BigDecimal original = obj.get();
		BigDecimal scaled = original.setScale(19, UNNECESSARY);
	    if (scaled.precision() > 38)
	        throw new ArithmeticException("BigDecimal can have a maximum of 19 digits both at the left and at the right of decimal point: " + obj);

	    row.setInt(start, original.scale());
	    row.setDecimal(start + 1, Decimal.apply(obj.get(), 38, 19), 38);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<BigDecimalValue<?>> userClass()
	{
		return (Class<BigDecimalValue<?>>) (Class<?>) BigDecimalValue.class;
	}
}
