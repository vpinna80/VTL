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

import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkEnvironment.LineageSparkUDT;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.createStructFromComponents;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.getScalarFor;
import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.sorter;
import static java.util.stream.Collectors.joining;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import it.bancaditalia.oss.vtl.impl.types.data.date.DayHolder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;

public class DataPointEncoder implements Serializable
{
	private static final long serialVersionUID = 1L;
	public final DataStructureComponent<?, ?, ?>[] components;
	public final DataSetMetadata structure;
	public final StructType schema;
	public final Encoder<Row> rowEncoder;
	public final Encoder<Row> rowEncoderNoLineage;
	
	public DataPointEncoder(Set<? extends DataStructureComponent<?, ?, ?>> dataStructure)
	{
		structure = dataStructure instanceof DataSetMetadata ? (DataSetMetadata) dataStructure : new DataStructureBuilder(dataStructure).build();
		components = structure.toArray(new DataStructureComponent<?, ?, ?>[structure.size()]);
		Arrays.sort(components, SparkUtils::sorter);
		List<StructField> fields = new ArrayList<>(createStructFromComponents(components));
		StructType schemaNoLineage = new StructType(fields.toArray(new StructField[components.length]));
		rowEncoderNoLineage = Encoders.row(schemaNoLineage);
		fields.add(new StructField("$lineage$", LineageSparkUDT, false, Metadata.empty()));
		schema = new StructType(fields.toArray(new StructField[components.length + 1]));
		rowEncoder = Encoders.row(schema);
	}

	public Row encode(DataPoint dp)
	{
		Serializable[] vals = new Serializable[components.length + 1];
		if (dp instanceof DataPointImpl)
		{
			DataPointImpl dpi = (DataPointImpl) dp;
			
			int k = 0;
			for (int i = 0; i < dpi.comps.length; i++)
				if (dpi.comps[i] != null)
				{
					vals[k] = dpi.vals[i].get();
					if (vals[k] instanceof DayHolder)
						vals[k] = ((DayHolder) vals[k]).getLocalDate();
					k++;
				}
		}
		else
			for (int i = 0; i < components.length; i++)
			{
				vals[i] = dp.get(components[i]).get();
				if (vals[i] instanceof DayHolder)
					vals[i] = ((DayHolder) vals[i]).getLocalDate();
			}
		
		vals[components.length] = dp.getLineage();

		return new GenericRowWithSchema(vals, schema);
	}

	public DataPointImpl decode(Row row)
	{
		ScalarValue<?, ?, ?, ?>[] vals = new ScalarValue<?, ?, ?, ?>[components.length];
		for (int i = 0; i < components.length; i++)
			vals[i] = getScalarFor(row.get(i), components[i]);
		
		Object lineage = row.get(components.length);
		if (lineage instanceof byte[])
			lineage = LineageSparkUDT.deserialize(row.get(components.length));
		
		return new DataPointImpl(components, vals, (Lineage) lineage);
	}

	public StructType getSchema()
	{
		return schema;
	}
	
	public Encoder<Row> getRowEncoder()
	{
		return rowEncoder;
	}

	public Encoder<Row> getRowEncoderNoLineage()
	{
		return rowEncoderNoLineage;
	}

	public DataSetMetadata getStructure()
	{
		return structure;
	}
	
	public static class DataPointImpl extends AbstractMap<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> implements DataPoint, Serializable 
	{
		private class EntrySetView extends AbstractSet<Entry<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>>
		{
			@Override
			public int size()
			{
				return DataPointImpl.this.size;
			}

			@Override
			public Iterator<Entry<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>> iterator()
			{
				return new Iterator<Entry<DataStructureComponent<?,?,?>,ScalarValue<?,?,?,?>>>() {
					int i = 0;
					
					@Override
					public Entry<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> next()
					{
						int cur = i++;
						hasNext();
						return new SimpleEntry<>(comps[cur], vals[cur]);
					}
					
					@Override
					public boolean hasNext()
					{
						while (i < size && comps[i] == null)
							i++;
						
						return i < size && comps[i] != null;
					}
				};
			}
		}

		private static final long serialVersionUID = 1L;
		
		private final DataStructureComponent<?, ?, ?>[] comps;
		private final ScalarValue<?, ?, ?, ?>[] vals;
		private final Lineage lineage;
		private final int size;
		private final int hashCode;
		private final EntrySetView view = new EntrySetView();


		public DataPointImpl(DataStructureComponent<?, ?, ?>[] comps, ScalarValue<?, ?, ?, ?>[] vals, Lineage lineage)
		{
			this.comps = comps;
			this.vals = vals;
			this.lineage = lineage;
			
			int size = 0;
			for (int i = 0; i < comps.length; i++)
				if (comps[i] != null)
					size++;
			this.size = size;
			hashCode = super.hashCode();
		}
		
		@Override
		public DataPoint dropComponents(Collection<? extends DataStructureComponent<? extends NonIdentifier, ?, ?>> components)
		{
			DataStructureComponent<?, ?, ?>[] comps2 = Arrays.copyOf(comps, comps.length);
			for (int i = 0; i < comps2.length; i++)
				if (comps2[i] != null && comps2[i].is(NonIdentifier.class) && components.contains(comps2[i]))
					comps2[i] = null;
			
			return new DataPointImpl(comps2, vals, lineage);
		}

		@Override
		public DataPoint keep(Collection<? extends DataStructureComponent<? extends NonIdentifier, ?, ?>> components)
		{
			DataStructureComponent<?, ?, ?>[] comps2 = Arrays.copyOf(comps, comps.length);
			for (int i = 0; i < comps2.length; i++)
				if (comps2[i] != null && comps2[i].is(NonIdentifier.class) && !components.contains(comps2[i]))
					comps2[i] = null;
			
			return new DataPointImpl(comps2, vals, lineage);
		}

		@Override
		public DataPoint renameComponent(DataStructureComponent<?, ?, ?> oldComponent, DataStructureComponent<?, ?, ?> newComponent)
		{
			DataStructureComponent<?, ?, ?>[] comps2 = Arrays.copyOf(comps, comps.length);
			for (int i = 0; i < comps2.length; i++)
				if (comps2[i] != null && oldComponent.equals(comps2[i]))
					comps2[i] = newComponent;
			
			return new DataPointImpl(comps2, vals, lineage);
		}

		@Override
		public DataPoint combine(DataPoint other, SerBiFunction<DataPoint, DataPoint, Lineage> lineageCombiner)
		{
			DataPointImpl dpo = (DataPointImpl) other;
			
			DataStructureComponent<?, ?, ?>[] comps2 = new DataStructureComponent<?, ?, ?>[comps.length + dpo.comps.length];
			ScalarValue<?, ?, ?, ?>[] vals2 = new ScalarValue<?, ?, ?, ?>[comps.length + dpo.comps.length];
			
			int i = 0, j = 0, k = 0;
			
			// Merge sorted components and values
			while (k < comps.length + dpo.comps.length)
			{
				while (i < comps.length && comps[i] == null)
					i++;
				while (j < dpo.comps.length && dpo.comps[j] == null)
					j++;
				
				int compare = i < comps.length ? j < dpo.comps.length ? sorter(comps[i], dpo.comps[j]) : -1 : 1;
				if (compare < 0)
				{
					comps2[k] = comps[i];  
					vals2[k++] = vals[i++];  
				}
				else
				{
					comps2[k] = dpo.comps[j];  
					vals2[k++] = dpo.vals[j++];  
				}
			}

			// Remove consecutive equal components
			for (i = 0; i < comps2.length; i++)
				if (comps2[i] != null)
					for (j = i + 1; j < comps2.length; j++)
						if (comps2[j] != null && comps2[i].equals(comps2[j]))
						{
							comps2[j] = null;
							j = comps2.length;
						}

			return new DataPointImpl(comps2, vals2, lineageCombiner.apply(this, dpo));
		}

		@Override
		public <R extends ComponentRole> Map<DataStructureComponent<R, ?, ?>, ScalarValue<?, ?, ?, ?>> getValues(Class<R> role)
		{
			Map<DataStructureComponent<R, ?, ?>, ScalarValue<?, ?, ?, ?>> map = new HashMap<>();

			for (int i = 0; i < comps.length; i++)
				if (comps[i] != null && comps[i].is(role))
					map.put(comps[i].asRole(role), vals[i]);
			
			return map;
		}
		
		@Override
		public ScalarValue<?, ?, ?, ?> get(Object key)
		{
			if (key instanceof DataStructureComponent)
				for (int i = 0; i < comps.length; i++)
					if (comps[i] != null && comps[i].equals(key))
						return vals[i];
			
			return null;
		}

		@Override
		public Lineage getLineage()
		{
			return lineage;
		}

		@Override
		public DataPoint enrichLineage(SerUnaryOperator<Lineage> enricher)
		{
			return new DataPointImpl(comps, vals, enricher.apply(lineage));
		}

		@Override
		public Set<Entry<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>> entrySet()
		{
			return view;
		}

		@Override
		public int hashCode()
		{
			return hashCode;
		}

		@Override
		public boolean equals(Object obj)
		{
			return super.equals(obj);
		}

		@Override
		public String toString()
		{
			return IntStream.range(0, comps.length)
				.filter(i -> comps[i] != null)
				.mapToObj(i -> comps[i] + "=" + vals[i])
				.collect(joining(", ", "{ ", " }"));
		}
		
		@Override
		public Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> getValues(Collection<? extends DataStructureComponent<?, ?, ?>> components)
		{
			Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> result = new HashMap<>();
			for (int i = 0; i < comps.length; i++)
				if (comps[i] != null && !components.contains(comps[i]))
					result.put(comps[i], vals[i]);
			
			return result;
		}
		
		DataStructureComponent<?, ?, ?> getComp(int i)
		{
			return comps[i];
		}
		
		ScalarValue<?, ?, ?, ?> getValue(int i)
		{
			return vals[i];
		}
	}
}
