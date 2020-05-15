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
package it.bancaditalia.oss.vtl.impl.engine.testutils;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.exceptions.VTLUnboundNameException;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

/**
 * A dummy session used for testing purposes.
 * No statements can be added to it, but it has a {@link #put(String, VTLValue)} method to bind values.
 * 
 * @author m027907
 *
 */
public class DummySession implements TransformationScheme
{
	private Map<String, VTLValue> values = new ConcurrentHashMap<>();

	public void put(String name, VTLValue value)
	{
		values.put(name, value);
	}
	
	@Override
	public VTLValue resolve(String node)
	{
		return Optional.ofNullable(values.get(node)).orElseThrow(() -> new VTLUnboundNameException(node));
	}

	@Override
	public VTLValueMetadata getMetadata(String node)
	{
		return Optional.ofNullable(values.get(node))
				.map(VTLValue::getMetadata)
				.orElseThrow(() -> new VTLUnboundNameException(node));
	}

	@Override
	public Statement getRule(String node)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public MetadataRepository getRepository()
	{
		throw new UnsupportedOperationException();
	}
}
