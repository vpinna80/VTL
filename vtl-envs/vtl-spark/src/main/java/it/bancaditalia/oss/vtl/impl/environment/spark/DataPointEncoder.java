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

import static it.bancaditalia.oss.vtl.impl.environment.spark.SparkUtils.createStructFromComponents;
import static it.bancaditalia.oss.vtl.impl.environment.spark.udts.LineageSparkUDT.LineageSparkUDT;
import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.types.DataTypes.createStructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;

public class DataPointEncoder implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final DataStructureComponent<?, ?, ?>[] components;
	private final StructType schema;
	private final Encoder<Row> rowEncoder;
	private final Encoder<Row> rowEncoderNoLineage;
	
	public DataPointEncoder(SparkSession session, Set<? extends DataStructureComponent<?, ?, ?>> structure)
	{
		// Ensures that SQL configuration is correct between all threads
		// Avoids ClassCastException LocalDate -> java.sql.Date
		SparkEnvironment.ensureConf(requireNonNull(session));
		
		components = structure.toArray(new DataStructureComponent<?, ?, ?>[structure.size()]);
		Arrays.sort(components, DataStructureComponent::byNameAndRole);
		List<StructField> fields = new ArrayList<>(createStructFromComponents(structure));
		StructType schemaNoLineage = createStructType(new ArrayList<>(fields));
		rowEncoderNoLineage = Encoders.row(schemaNoLineage);
		fields.add(new StructField("$lineage$", LineageSparkUDT, false, Metadata.empty()));
		schema = createStructType(fields);
		rowEncoder = Encoders.row(schema);
	}

	public Row encode(DataPoint dp)
	{
		Serializable[] vals = new Serializable[components.length + 1];
		for (int i = 0; i < components.length; i++)
		{
			vals[i] = dp.get(components[i]);
			if (((ScalarValue<?, ?, ?, ?>) vals[i]).isNull())
				vals[i] = null;
		}
		
		for (int i = 0; i < vals.length; i++)
			if (vals[i] instanceof NullValue)
				throw new NullPointerException();
		
		vals[components.length] = dp.getLineage();
		return new GenericRowWithSchema(vals, schema);
	}

	public SparkDataPoint decode(Row row, int start)
	{
		Lineage lineage = row.getAs(components.length + start);
		ScalarValue<?, ?, ?, ?>[] vals = new ScalarValue<?, ?, ?, ?>[components.length]; 
		for (int i = 0; i < components.length; i++)
			vals[i] = row.isNullAt(i + start) ? NullValue.instance(components[i].getVariable().getDomain()) : row.getAs(i + start);

		for (int i = 0; i < components.length; i++)
			if (vals[i] == null)
				throw new NullPointerException();

		return new SparkDataPoint(components, vals, lineage);
	}

	public SparkDataPoint decode(Row row)
	{
		return decode(row, 0);
	}

	public StructType getSchema()
	{
		return schema;
	}
	
	public Encoder<Row> getRowEncoder()
	{
		return rowEncoder;
	}
	
	public DataStructureComponent<?, ?, ?>[] getComponents()
	{
		return components;
	}

	public Encoder<Row> getRowEncoderNoLineage()
	{
		return rowEncoderNoLineage;
	}
}
