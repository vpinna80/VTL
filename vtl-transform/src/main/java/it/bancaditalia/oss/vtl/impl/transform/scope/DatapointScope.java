/*******************************************************************************
 * Copyright 2020, Bank Of Italy
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
 *******************************************************************************/
package it.bancaditalia.oss.vtl.impl.transform.scope;

import static it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope.THIS;

import java.util.Objects;

import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.exceptions.VTLUnboundNameException;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.ScalarValue.VTLScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.Utils;

public class DatapointScope implements TransformationScheme
{
	private final DataPoint dp;
	private final VTLDataSetMetadata structure;
	private final TransformationScheme parent;
	
	public DatapointScope(DataPoint dp, VTLDataSetMetadata structure, TransformationScheme parent) 
	{
		this.dp = dp;
		this.structure = structure;
		this.parent = parent;
	}

	@Override
	public VTLValue resolve(String name) 
	{
		if (dp == null)
			throw new IllegalStateException("Can't resolve values during compilation.");
//		if (alias.equals(name))
//			return Optional.of(new LightDataSet(alias, structure, () -> Stream.of(dp)));
//		else if (THIS.equals(name))
//			return new LightDataSet(THIS, new Builder(dp.keySet()).build(), () -> Stream.of(dp));
		else if (Objects.requireNonNull(name, "The name to resolve cannot be null.").matches("'.*'"))
			return Utils.getStream(dp.keySet()).filter(c -> c.getName().equals(name.replaceAll("'(.*)'", "$1"))).findAny().map(dp::get).orElseThrow(() -> new VTLUnboundNameException(name));
		else
			return Utils.getStream(dp.keySet()).filter(c -> c.getName().equalsIgnoreCase(name)).findAny().map(dp::get).orElseThrow(() -> new VTLUnboundNameException(name));
	}

	@Override
	public VTLValueMetadata getMetadata(String name)
	{
		if (Objects.requireNonNull(name, "The name to resolve cannot be null.").matches("'.*'"))
			return (VTLScalarValueMetadata<?>) () -> Utils.getStream(structure).filter(c -> c.getName().equals(name.replaceAll("'(.*)'", "$1"))).findAny().orElseThrow(() -> new VTLUnboundNameException(name)).getDomain();
		else if (THIS.equals(name))
			return structure;
		else
			return (VTLScalarValueMetadata<?>) () -> Utils.getStream(structure).filter(c -> c.getName().equalsIgnoreCase(name)).findAny().orElseThrow(() -> new VTLUnboundNameException(name)).getDomain();
	}

	@Override
	public Statement getRule(String node)
	{
		return parent.getRule(node);
	}

	@Override
	public MetadataRepository getRepository()
	{
		throw new UnsupportedOperationException();
	}
}
