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
package it.bancaditalia.oss.vtl.impl.transform.ops;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toConcurrentMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightFDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.NamedDataSet;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class SetTransformation extends TransformationImpl
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SetTransformation.class);
	private static final long serialVersionUID = 1L;

	public enum SetOperator
	{
		UNION((left, right) -> (structure) -> new LightFDataSet<>(structure, 
				ds -> Stream.concat(left.stream(), ds.stream()), setDiff(right, left))),
		INTERSECT((left, right) -> (structure) -> setDiff(left, setDiff(left, right))), 
		SETDIFF((left, right) -> (structure) -> setDiff(left, right)), 
		SYMDIFF((left, right) -> (structure) -> new LightFDataSet<>(structure,  
				ds -> Stream.concat(setDiff(left, right).stream(), ds.stream()), setDiff(right, left)));

		private final BiFunction<DataSet, DataSet, Function<DataSetMetadata, DataSet>> reducer;

		SetOperator(BiFunction<DataSet, DataSet, Function<DataSetMetadata, DataSet>> reducer)
		{
			this.reducer = reducer;
		}
		
		public BinaryOperator<DataSet> getReducer(DataSetMetadata metadata)
		{
			return (left, right) -> reducer.apply(left, right).apply(metadata);
		}
	}

	private static DataSet setDiff(DataSet left, DataSet right)
	{
		Set<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>> index = extractIndex(right);
		
		return left.filter(dp -> !index.contains(dp.getValues(Identifier.class)));
	}

	private static Set<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>> extractIndex(DataSet right)
	{
		String alias = right instanceof NamedDataSet ? ((NamedDataSet) right).getAlias() : "unnamed dataset";
		LOGGER.debug("Started indexing {}.", alias);
		Set<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>> index;
		try (Stream<DataPoint> stream = right.stream())
		{
			index = new HashSet<>(stream
				.map(dp -> dp.getValues(Identifier.class))
				.collect(toConcurrentMap(identity(), x -> Boolean.TRUE))
				.keySet());
		}
		LOGGER.debug("Finished indexing {}.", alias);
		return index;
	}

	private final List<Transformation> operands;
	private final SetOperator setOperator;

	public SetTransformation(SetOperator setOperator, List<Transformation> operands)
	{
		this.operands = operands;
		this.setOperator = setOperator;
	}

	@Override
	public DataSet eval(TransformationScheme scheme)
	{
		DataSet accumulator = null;
		boolean first = true;
		for (Transformation operand: operands)
		{
			DataSet other = (DataSet) operand.eval(scheme);
			
			if (first)
			{
				first = false;
				accumulator = other;
			}
			else
				accumulator = setOperator.getReducer(accumulator.getMetadata()).apply(accumulator, other);
		}

		return accumulator;
	}

	@Override
	public DataSetMetadata getMetadata(TransformationScheme scheme)
	{
		List<VTLValueMetadata> meta = operands.stream()
				.map(t -> t.getMetadata(scheme))
				.collect(toList());
		
		if (!(meta.get(0) instanceof DataSetMetadata))
			throw new UnsupportedOperationException("In set operation expected all datasets but found a scalar"); 
			
		if (meta.stream().distinct().limit(2).count() != 1)
			throw new UnsupportedOperationException("In set operation expected all datasets with equal structure but found: " + meta); 

		return (DataSetMetadata) meta.get(0);
	}
	
	@Override
	public boolean isTerminal()
	{
		return false;
	}
	
	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return operands.stream().flatMap(t -> t.getTerminals().stream()).collect(toSet());
	}

	@Override
	public String toString()
	{
		return operands.stream().map(Object::toString).collect(joining(", ", setOperator + "(", ")"));
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((operands == null) ? 0 : operands.hashCode());
		result = prime * result + ((setOperator == null) ? 0 : setOperator.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (!(obj instanceof SetTransformation)) return false;
		SetTransformation other = (SetTransformation) obj;
		if (operands == null)
		{
			if (other.operands != null) return false;
		}
		else if (!operands.equals(other.operands)) return false;
		if (setOperator != other.setOperator) return false;
		return true;
	}
	
	@Override
	public Lineage computeLineage()
	{
		return LineageNode.of(this, operands.stream().map(Transformation::getLineage).collect(Collectors.toList()).toArray(new Lineage[0]));
	}
}
