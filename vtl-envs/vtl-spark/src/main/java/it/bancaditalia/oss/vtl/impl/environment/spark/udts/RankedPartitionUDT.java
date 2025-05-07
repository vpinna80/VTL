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
import static it.bancaditalia.oss.vtl.impl.environment.spark.scalars.ScalarValueUDT.getUDTForClass;
import static it.bancaditalia.oss.vtl.impl.environment.spark.udts.LineageSparkUDT.LineageSparkUDT;
import static org.apache.spark.sql.catalyst.util.ArrayData.toArrayData;
import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.createArrayType;
import static org.apache.spark.sql.types.DataTypes.createStructType;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UserDefinedType;
import org.objenesis.strategy.StdInstantiatorStrategy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import it.bancaditalia.oss.vtl.impl.environment.spark.SparkDataPoint;
import it.bancaditalia.oss.vtl.impl.environment.spark.scalars.ScalarValueUDT;
import it.bancaditalia.oss.vtl.impl.types.window.RankedPartition;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;

public class RankedPartitionUDT extends UserDefinedType<RankedPartition>
{
	private static final long serialVersionUID = 1L;
    private static final ThreadLocal<Kryo> KRYO = ThreadLocal.withInitial(() -> {
    	Kryo kryo = new Kryo();
    	kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
    	return kryo;
    });
    
    private final StructType sqlType;

	public RankedPartitionUDT()
	{
		sqlType = null;
	}
	
	public RankedPartitionUDT(Collection<? extends DataStructureComponent<?, ?, ?>> structure)
	{
		List<StructField> structFromComponents = new ArrayList<>(structure.size() * 2 + 1);
		
		int i = 0;
		for (StructField compField: createStructFromComponents(structure))
		{
			structFromComponents.add(new StructField("_$type$_" + ++i, IntegerType, true, Metadata.empty()));
			structFromComponents.add(compField);
		}
		structFromComponents.add(new StructField("$lineage$", LineageSparkUDT, false, Metadata.empty()));
		StructType dpStruct = createStructType(structFromComponents);
		
		StructField dpField = new StructField("datapoint", dpStruct, false, Metadata.empty());
		StructField rankField = new StructField("rank", LongType, false, Metadata.empty());
		StructType rankStruct = createStructType(List.of(dpField, rankField));

		sqlType = createStructType(List.of(
				new StructField("structure", BinaryType, false, Metadata.empty()),
				new StructField("data", createArrayType(rankStruct), false, Metadata.empty())
			));
	}
	
	@Override
	public Object serialize(RankedPartition obj)
	{
		Set<Entry<DataPoint, Long>> entrySet = obj.entrySet();
		GenericInternalRow[] rows = new GenericInternalRow[entrySet.size()];
		
		DataStructureComponent<?, ?, ?>[] components = null;
		if (!entrySet.isEmpty())
		{
			components = entrySet.iterator().next().getKey().keySet().toArray(DataStructureComponent<?, ?, ?>[]::new);
			Arrays.sort(components, DataStructureComponent::byNameAndRole);
			
			int j = 0;
			for (Entry<DataPoint, Long> rank: entrySet)
			{
				DataPoint dp = rank.getKey();
				Object[] vals = new Object[components.length * 2 + 1];
				for (int i = 0; i < components.length; i++)
				{
					ScalarValue<?, ?, ?, ?> val = dp.get(components[i]);
					if (val == null || val.isNull())
					{
						vals[i * 2] = null;
						vals[i * 2 + 1] = null;
					}
					else
					{
						Entry<Integer, ScalarValueUDT<?>> entry = getUDTForClass(val.getClass());
						vals[i * 2] = entry.getKey();
						vals[i * 2 + 1] = entry.getValue().serializeScalar(val);
					}
				}
					
				vals[vals.length - 1] = LineageSparkUDT.serialize(dp.getLineage());
				GenericInternalRow serializedDP = new GenericInternalRow(vals);
				
				rows[j++] = new GenericInternalRow(new Object[] { serializedDP, rank.getValue() });
			}
		}
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (Output output = new Output(baos))
		{
			KRYO.get().writeObject(output, components);
		}
		
		return new GenericInternalRow(new Object[] { baos.toByteArray(), toArrayData(rows) });
	}

	@Override
	public RankedPartition deserialize(Object datum)
	{
		DataStructureComponent<?, ?, ?>[] components = (DataStructureComponent<?, ?, ?>[]) KRYO.get()
				.readObject(new Input(((InternalRow) datum).getBinary(0)), DataStructureComponent[].class);
		ArrayData data = ((InternalRow) datum).getArray(1);

		RankedPartition ranks = new RankedPartition(data.numElements());

		for (int nRow = 0; nRow < data.numElements(); nRow++)
		{
			InternalRow entry = data.getStruct(nRow, 2);
			InternalRow row = (InternalRow) entry.getStruct(0, components.length * 2 + 1);
			ScalarValue<?, ?, ?, ?>[] values = new ScalarValue<?, ?, ?, ?>[components.length];
			for (int i = 0; i < components.length; i++)
				if (!row.isNullAt(i * 2))
				{
					ScalarValueUDT<?> udt = ScalarValueUDT.getUDTforTag(row.getInt(i * 2));
					ScalarValue<?, ?, ?, ?> scalar = (ScalarValue<?, ?, ?, ?>) udt.deserialize(row.get(i * 2 + 1, udt));
					values[i] = components[i].getVariable().getDomain().cast(scalar);
				}
			
			Lineage lineage = LineageSparkUDT.deserialize(row.getBinary(components.length * 2));
			DataPoint dp = new SparkDataPoint(components, values, lineage);
			ranks.put(dp, entry.getLong(1));
		}
		
		return ranks;
	}

	@Override
	public DataType sqlType()
	{
		return sqlType;
	}

	@Override
	public Class<RankedPartition> userClass()
	{
		return RankedPartition.class;
	}
}
