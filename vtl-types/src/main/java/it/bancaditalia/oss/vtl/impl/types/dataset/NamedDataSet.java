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
package it.bancaditalia.oss.vtl.impl.types.dataset;

import java.util.Map;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;

public class NamedDataSet extends AbstractDataSet
{
	private final DataSet delegate;
	private final VTLAlias alias;

	public NamedDataSet(VTLAlias alias, DataSet delegate)
	{
		super(delegate.getMetadata());
		
		while (delegate instanceof NamedDataSet)
		{
			NamedDataSet namedDelegate = (NamedDataSet) delegate;
			delegate = namedDelegate.delegate;
			alias = namedDelegate.alias;
		}
		
		this.alias = alias;
		this.delegate = delegate;
	}

	public VTLAlias getAlias()
	{
		return alias;
	}

	public DataSet getDelegate()
	{
		return delegate;
	}

	@Override
	public DataSet getMatching(Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>> keyValues)
	{
		return delegate.getMatching(keyValues);
	}

	@Override
	protected Stream<DataPoint> streamDataPoints()
	{
		return delegate.stream();
	}
	
	@Override
	public String toString()
	{
		return "" + alias + getMetadata();
	}
}
