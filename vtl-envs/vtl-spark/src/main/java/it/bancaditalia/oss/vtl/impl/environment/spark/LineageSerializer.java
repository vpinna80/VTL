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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import it.bancaditalia.oss.vtl.impl.types.lineage.LineageCall;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageExternal;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageSet;
import it.bancaditalia.oss.vtl.model.data.Lineage;

public class LineageSerializer extends Serializer<Lineage>
{
	private static final Map<Class<? extends Lineage>, Integer> SERIALIZER_TAGS = new HashMap<>();
	
	static {
		SERIALIZER_TAGS.put(LineageExternal.class, 1);
		SERIALIZER_TAGS.put(LineageCall.class, 2);
		SERIALIZER_TAGS.put(LineageNode.class, 4);
	}
	
	@Override
	public void write(Kryo kryo, Output output, Lineage lineage)
	{
		output.writeInt(SERIALIZER_TAGS.get(lineage.getClass()));
		if (lineage instanceof LineageExternal)
			output.writeString(lineage.toString());
		else if (lineage instanceof LineageCall)
		{
			List<Lineage> sources = ((LineageCall) lineage).getSources();
			output.writeInt(sources.size());
			sources.forEach(l -> write(kryo, output, l));
		}
		else if (lineage instanceof LineageNode)
		{
			output.writeString(((LineageNode) lineage).getTransformation());
			write(kryo, output, ((LineageNode) lineage).getSourceSet());
		}
		else
			throw new UnsupportedOperationException("Unrecognized lineage class for serialization: " + lineage.getClass());
	}

	@Override
	public Lineage read(Kryo kryo, Input input, Class<Lineage> lineageType)
	{
		int type = input.readInt();
		int n;
		switch (type) 
		{
			case 1:
				return LineageExternal.of(input.readString());
			case 2:
				n = input.readInt();
				List<Lineage> list = new ArrayList<>();
				for (int i = 0; i < n; i++)
					list.add(read(kryo, input, lineageType));
				return LineageCall.of(list);
			case 4:
				String transformation = input.readString();
				LineageSet source = (LineageSet) read(kryo, input, lineageType);
				return LineageNode.of(transformation, source);
			default:
				throw new UnsupportedOperationException();
		}
	}
}