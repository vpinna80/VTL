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

import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.ref.SoftReference;
import java.util.Map;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;

public class NamedDataSet extends AbstractDataSet
{
	private static final long serialVersionUID = 1L;

	private final DataSet delegate;
	private final String alias;

	private SoftReference<String> cacheString  = null;

	public NamedDataSet(String alias, DataSet delegate)
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

	public String getAlias()
	{
		return alias;
	}

	public void streamTo(PrintWriter output)
	{
		output.println(alias + " = {");
		output.print("\t");
		try (Stream<DataPoint> stream = stream())
		{
			stream.forEach(dp -> {
				String datapoint = dp.toString();
				output.println(",");
				output.print("\t");
				output.print(datapoint);
			});
		}
		output.println();
		output.println("}");
	}

	@Override
	public String toString()
	{
		String result = null;
		if (cacheString != null)
			result = cacheString.get();

		if (result != null)
			return result;

		result = alias + " = " + getDelegate();

		cacheString = new SoftReference<>(result);
		return result;
	}

	public void streamTo(PrintStream output)
	{
		streamTo(new PrintWriter(new OutputStreamWriter(output)));
	}

	public DataSet getDelegate()
	{
		return delegate;
	}

	@Override
	public DataSet getMatching(Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>> keyValues)
	{
		return delegate.getMatching(keyValues);
	}

	@Override
	protected Stream<DataPoint> streamDataPoints()
	{
		// TODO Auto-generated method stub
		return delegate.stream();
	}
}
