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

import com.esotericsoftware.kryo.Kryo;

import it.bancaditalia.oss.vtl.impl.types.lineage.LineageCall;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageExternal;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageGroup;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageImpl;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageSet;
import it.bancaditalia.oss.vtl.model.data.Lineage;

public class DataPointSerializer$ extends DataPointSerializer
{
	public static DataPointSerializer$ MODULE$ = new DataPointSerializer$();
	public static final Kryo KRYO;
	
	static {
		KRYO = new Kryo();
		DataPointSerializer datapointSerializer = new DataPointSerializer();
		KRYO.register(LineageExternal.class, datapointSerializer);
		KRYO.register(LineageGroup.class, datapointSerializer);
		KRYO.register(LineageCall.class, datapointSerializer);
		KRYO.register(LineageNode.class, datapointSerializer);
		KRYO.register(LineageImpl.class, datapointSerializer);
		KRYO.register(LineageSet.class, datapointSerializer);
		KRYO.register(Lineage.class, datapointSerializer);
	}
	
	public static DataPointSerializer$ apply()
	{
		return MODULE$;
	}
	
	@Override
	protected Kryo getKryoInstance()
	{
		return KRYO;
	}
}
