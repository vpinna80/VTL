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

import it.bancaditalia.oss.vtl.impl.types.data.DurationValue;
import it.bancaditalia.oss.vtl.impl.types.data.Frequency;

public class DurationValueUDT extends SingleFieldScalarValueUDT<DurationValue>
{
	private static final long serialVersionUID = 1L;
	private static final Frequency[] FREQS = Frequency.values();
	
	public DurationValueUDT()
	{
		super(IntegerType);
	}

	@Override
	public DurationValue deserializeInternal(Object datum)
	{
		int tag = (Integer) datum;
		return tag >= 0 ? FREQS[tag].get() : null;
	}

	@Override
	public Object serializeInternal(DurationValue value)
	{
		return value != null ? value.get().ordinal() : -1;
	}

	@Override
	public Class<DurationValue> userClass()
	{
		return DurationValue.class;
	}
}
