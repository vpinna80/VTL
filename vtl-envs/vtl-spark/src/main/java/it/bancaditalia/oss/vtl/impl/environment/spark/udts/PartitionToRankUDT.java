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
package it.bancaditalia.oss.vtl.impl.environment.spark.udts;

import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.createStructFromComponents;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.getDataTypeFor;
import static it.bancaditalia.oss.vtl.impl.environment.spark.udts.LineageUDT.LineageSparkUDT;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toArray;
import static org.apache.spark.sql.catalyst.util.ArrayData.toArrayData;
import static org.apache.spark.sql.types.DataTypes.createArrayType;
import static org.apache.spark.sql.types.DataTypes.createStructType;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.UserDefinedType;

import it.bancaditalia.oss.vtl.impl.environment.spark.scalars.ScalarValueUDT;
import it.bancaditalia.oss.vtl.impl.types.operators.PartitionToRank;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;

public class PartitionToRankUDT extends UserDefinedType<PartitionToRank>
{
	private static final long serialVersionUID = 1L;

	private final DataType sqlType;

	public PartitionToRankUDT(DataSetMetadata structure)
	{
		List<StructField> structFromComponents = createStructFromComponents(structure);
		structFromComponents.add(new StructField("$lineage$", LineageSparkUDT, false, Metadata.empty()));
		sqlType = createStructType(List.of(new StructField("udtfield", 
				createArrayType(createStructType(structFromComponents)), true, Metadata.empty())));
	}
	
	@Override
	public PartitionToRank deserialize(Object datum)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public Object serialize(PartitionToRank obj)
	{
		if (obj.isEmpty())
			return new GenericInternalRow[] { new GenericInternalRow(new Object[] { null }) };
		
		DataStructureComponent<?, ?, ?>[] components = obj.iterator().next().keySet().toArray(DataStructureComponent<?, ?, ?>[]::new);
		Arrays.sort(components, DataStructureComponent::byNameAndRole);
		@SuppressWarnings("unchecked")
		ScalarValueUDT<ScalarValue<?, ?, ?, ?>>[] types = (ScalarValueUDT<ScalarValue<?, ?, ?, ?>>[]) Arrays.stream(components).map(c -> (ScalarValueUDT<?>) getDataTypeFor(c)).collect(toArray(new ScalarValueUDT<?>[components.length]));
		
		GenericInternalRow[] rows = new GenericInternalRow[obj.size()];
		int j = 0;
		for (DataPoint dp: obj)
		{
			Object[] vals = new Object[types.length];
			for (int i = 0; i < components.length; i++)
			{
				ScalarValue<?, ?, ?, ?> val = dp.get(components[i]);
				vals[i] = val == null || val.isNull() ? null : types[i].serialize(val);
			}
			
			vals[vals.length - 1] = LineageSparkUDT.serialize(dp.getLineage());
			rows[j++] = new GenericInternalRow(vals);
		}
		
		return new GenericInternalRow(new Object[] { toArrayData(rows) });
	}

	@Override
	public DataType sqlType()
	{
		return sqlType;
	}

	@Override
	public Class<PartitionToRank> userClass()
	{
		return PartitionToRank.class;
	}
}
