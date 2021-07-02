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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NULLDS;
import static it.bancaditalia.oss.vtl.model.data.DataStructureComponent.byName;
import static it.bancaditalia.oss.vtl.util.Utils.splittingConsumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.DateHolder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Roles;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;

public class DataPointSerializer extends Serializer<DataPoint>
{
	private static final Map<Class<?>, Integer> DOMAIN_TAGS = new HashMap<>();
	
	protected DataPointSerializer()
	{

	}
	
	static {
		DOMAIN_TAGS.put(IntegerValue.class, 1);
		DOMAIN_TAGS.put(DoubleValue.class, 2);
		DOMAIN_TAGS.put(StringValue.class, 3);
		DOMAIN_TAGS.put(BooleanValue.class, 4);
		DOMAIN_TAGS.put(NullValue.class, 5);
		DOMAIN_TAGS.put(DateValue.class, 6);
	}
	
	static ScalarValue<?, ?, ?, ?> readScalar(int tag, Kryo kryo, Input input)
	{
		switch (tag)
		{
			case 1: return IntegerValue.of(input.readLong());
			case 2: return DoubleValue.of(input.readDouble());
			case 3: return StringValue.of(input.readString());
			case 4: return BooleanValue.of(input.readBoolean());
			case 5: return NullValue.instance(NULLDS);
			case 6: return DateValue.of((DateHolder<?>) kryo.readClassAndObject(input));
			default: throw new IllegalStateException();
		}
	}
	
	static void writeScalar(ScalarValue<?, ?, ?, ?> scalar, int tag, Kryo kryo, Output output)
	{
		switch (tag)
		{
			case 1: output.writeLong((Long) scalar.get()); break;
			case 2: output.writeDouble((Double) scalar.get()); break;
			case 3: output.writeString((String) scalar.get()); break;
			case 4: output.writeBoolean((Boolean) scalar.get()); break;
			case 5: break;
			case 6: kryo.writeClassAndObject(output, scalar.get()); break;
		}
	}

	public void write(Output output, DataPoint dp)
	{
		write(getKryoInstance(), output, dp);
	}

	@Override
	public void write(Kryo kryo, Output output, DataPoint dp)
	{
		output.writeVarInt(dp.size(), true);
		dp.entrySet().stream()
			.sorted((dp1, dp2) -> byName().compare(dp1.getKey(), dp2.getKey()))
			.forEach(splittingConsumer((k, v) -> {
				Integer tag = DOMAIN_TAGS.get(v.getClass());
				Objects.requireNonNull(tag, v.getClass().toString());
				output.writeVarInt(tag, true);
				writeScalar(v, tag, kryo, output);
				output.writeString(k.getName());
				kryo.writeObject(output, Roles.from(k.getRole()));
			}));
		kryo.writeClassAndObject(output, dp.getLineage());
	}

	public DataPoint read(Input input, Class<DataPoint> type)
	{
		return read(getKryoInstance(), input, type);
	}
	
	@Override
	public DataPoint read(Kryo kryo, Input input, Class<DataPoint> type)
	{
		Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> values = new HashMap<>();
		int size = input.readVarInt(true);
		for (int i = 0; i < size; i++)
		{
			ScalarValue<?, ?, ?, ?> value = Objects.requireNonNull(readScalar(input.readVarInt(true), kryo, input));
			String name = Objects.requireNonNull(input.readString());
			ValueDomainSubset<?, ?> domain = Objects.requireNonNull(value.getDomain());
			DataStructureComponent<?, ?, ?> component = DataStructureComponentImpl.of(name, kryo.readObject(input, Roles.class).getClazz(), domain);
			values.put(component, value);
		}
		return new SparkDataPoint((Lineage) kryo.readClassAndObject(input), values);
	}
	
	protected Kryo getKryoInstance()
	{
		throw new UnsupportedOperationException();
	}
}