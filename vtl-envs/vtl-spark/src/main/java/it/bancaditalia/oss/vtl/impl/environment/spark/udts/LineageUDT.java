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

import static org.apache.spark.sql.types.DataTypes.BinaryType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.UserDefinedType;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import it.bancaditalia.oss.vtl.impl.environment.spark.LineageSerializer;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageCall;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageExternal;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageImpl;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageSet;
import it.bancaditalia.oss.vtl.model.data.Lineage;

public class LineageUDT extends UserDefinedType<Lineage>
{
	private static final long serialVersionUID = 1L;
	public static final LineageUDT LineageSparkUDT = new LineageUDT();

    public static final ThreadLocal<Kryo> KRYO = new ThreadLocal<Kryo>() {

    	@Override
    	protected Kryo initialValue() {
    		Kryo kryo = new Kryo();
    		LineageSerializer lineageSerializer = new LineageSerializer();
    		kryo.register(LineageExternal.class, lineageSerializer);
    		kryo.register(LineageCall.class, lineageSerializer);
    		kryo.register(LineageNode.class, lineageSerializer);
    		kryo.register(LineageImpl.class, lineageSerializer);
    		kryo.register(LineageSet.class, lineageSerializer);
    		kryo.register(Lineage.class, lineageSerializer);
    		
    		return kryo;
    	}
    };

	@Override
	public Lineage deserialize(Object datum)
	{
		return (Lineage) getKryoInstance().readClassAndObject(new Input(new ByteArrayInputStream((byte[]) datum)));
	}

	@Override
	public byte[] serialize(Lineage obj)
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		Output output = new Output(baos);
		getKryoInstance().writeClassAndObject(output, obj);
		output.close();
		return baos.toByteArray();
	}

	@Override
	public DataType sqlType()
	{
		return BinaryType;
	}

	@Override
	public Class<Lineage> userClass()
	{
		return Lineage.class;
	}
	
	protected Kryo getKryoInstance()
	{
		return KRYO.get();
	}

	@Override
	public String toString()
	{
		return "LineageUDT";
	}
	
	@Override
	public String typeName()
	{
		return toString();
	}
}