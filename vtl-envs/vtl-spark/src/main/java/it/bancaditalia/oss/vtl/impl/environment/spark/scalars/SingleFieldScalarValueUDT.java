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

import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;

import it.bancaditalia.oss.vtl.model.data.ScalarValue;

public abstract class SingleFieldScalarValueUDT<T extends ScalarValue<?, ?, ?, ?>> extends ScalarValueUDT<T>
{
	private static final long serialVersionUID = 1L;
	
	private final DataType scalarType;

	public SingleFieldScalarValueUDT(DataType sqlType)
	{
		super(createStructType(List.of(createStructField("value", sqlType, true))));
		
		this.scalarType = sqlType;
	}

	@Override
	public final T deserialize(Object datum)
	{
		InternalRow row = (InternalRow) datum;
		return row == null ? null : deserializeInternal(row.isNullAt(0) ? null : row.get(0, scalarType));
	}
	
	@Override
	public final InternalRow serialize(T obj)
	{
		return new GenericInternalRow(new Object[] { serializeInternal(obj) }); 
	}

	protected abstract T deserializeInternal(Object object);

	protected abstract Object serializeInternal(T obj);
}
