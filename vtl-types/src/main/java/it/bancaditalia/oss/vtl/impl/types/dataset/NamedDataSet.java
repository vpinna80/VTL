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
package it.bancaditalia.oss.vtl.impl.types.dataset;

import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.ref.SoftReference;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;

public class NamedDataSet implements DataSet
{
	private static final long serialVersionUID = 1L;

	private final DataSet delegate;
	private final String alias;

	private SoftReference<String> cacheString  = null;

	public NamedDataSet(String alias, DataSet delegate)
	{
		while (delegate instanceof NamedDataSet)
		{
			NamedDataSet namedDelegate = (NamedDataSet) delegate;
			delegate = namedDelegate.delegate;
			alias = namedDelegate.alias;
		}
		
		this.alias = alias;
		this.delegate = delegate;
	}

	public VTLDataSetMetadata getMetadata()
	{
		return getDelegate().getMetadata();
	}

	public long size()
	{
		return getDelegate().size();
	}

	public Stream<DataPoint> stream()
	{
		return delegate.stream();
	}

	public VTLDataSetMetadata getDataStructure()
	{
		return getDelegate().getDataStructure();
	}

	public Collection<? extends DataStructureComponent<?, ?, ?>> getComponents()
	{
		return getDelegate().getComponents();
	}

	public <R extends ComponentRole> Set<DataStructureComponent<R, ?, ?>> getComponents(Class<R> typeOfComponent)
	{
		return getDelegate().getComponents(typeOfComponent);
	}

	public <R extends ComponentRole, S extends ValueDomainSubset<D>, D extends ValueDomain> Set<DataStructureComponent<R, S, D>> getComponents(
			Class<R> typeOfComponent, S domain)
	{
		return getDelegate().getComponents(typeOfComponent, domain);
	}

	public DataSet membership(String component)
	{
		return getDelegate().membership(component);
	}

	public Optional<DataStructureComponent<?, ?, ?>> getComponent(String name)
	{
		return getDelegate().getComponent(name);
	}

	public <S extends ValueDomainSubset<D>, D extends ValueDomain> DataStructureComponent<?, S, D> getComponent(String name, S domain)
	{
		return getDelegate().getComponent(name, domain);
	}

	public <R extends ComponentRole, S extends ValueDomainSubset<D>, D extends ValueDomain> DataStructureComponent<R, S, D> getComponent(String name,
			Class<R> typeOfComponent, S domain)
	{
		return getDelegate().getComponent(name, typeOfComponent, domain);
	}

	public Stream<DataPoint> getMatching(Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>> keyValues)
	{
		return getDelegate().getMatching(keyValues);
	}

	public boolean contains(DataPoint datapoint)
	{
		return getDelegate().contains(datapoint);
	}

	public boolean notContains(DataPoint datapoint)
	{
		return getDelegate().notContains(datapoint);
	}

	public DataSet filteredMappedJoin(VTLDataSetMetadata metadata, DataSet rightDataset, BiPredicate<DataPoint, DataPoint> filter,
			BinaryOperator<DataPoint> mergeOp)
	{
		return getDelegate().filteredMappedJoin(metadata, rightDataset, filter, mergeOp);
	}

	public <T> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys,
			Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>> filter,
			BiFunction<? super Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>>, ? super Stream<DataPoint>, T> groupMapper)
	{
		return getDelegate().streamByKeys(keys, filter, groupMapper);
	}

	public <T> Stream<T> streamByKeys(Set<DataStructureComponent<Identifier, ?, ?>> keys,
			BiFunction<? super Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?>>, ? super Stream<DataPoint>, T> groupMapper)
	{
		return getDelegate().streamByKeys(keys, groupMapper);
	}

	public DataSet filter(Predicate<DataPoint> predicate)
	{
		return getDelegate().filter(predicate);
	}

	public DataSet mapKeepingKeys(VTLDataSetMetadata metadata,
			Function<? super DataPoint, ? extends Map<? extends DataStructureComponent<? extends NonIdentifier, ?, ?>, ? extends ScalarValue<?, ?, ?>>> operator)
	{
		return getDelegate().mapKeepingKeys(metadata, operator);
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
}
