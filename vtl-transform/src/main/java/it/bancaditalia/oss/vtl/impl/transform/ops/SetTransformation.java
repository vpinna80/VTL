/**
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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightFDataSet;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class SetTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;

	public enum SetOperator
	{
		UNION((left, right) -> structure -> new LightFDataSet<>(structure, 
				ds -> Stream.concat(left.stream(), ds.stream()), setDiff(right, left))),
		INTERSECT((left, right) -> structure -> setDiff(left, setDiff(right, left))), 
		SETDIFF((left, right) -> structure -> setDiff(left, right)), 
		SYMDIFF((left, right) -> structure -> new LightFDataSet<>(structure,  
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

	private static DataSet setDiff(DataSet a, DataSet b)
	{
		Map<?, ?> leftValues = b.stream().collect(toMap(dp -> dp.getValues(Identifier.class), dp -> dp));
		
		return a.filter(dp -> !leftValues.containsKey(dp.getValues(Identifier.class)));
	}

	private final List<Transformation> operands;
	private final SetOperator setOperator;

	private DataSetMetadata metadata;
	
	public SetTransformation(SetOperator setOperator, List<Transformation> operands)
	{
		this.operands = operands;
		this.setOperator = setOperator;
	}

	@Override
	public DataSet eval(TransformationScheme scheme)
	{
		DataSet accumulator = null;
		AtomicBoolean first = new AtomicBoolean(true);
		for (Transformation operand: operands)
		{
			DataSet other = (DataSet) operand.eval(scheme);
			
			if (first.getAndSet(false))
				accumulator = other;
			else
			{
				DataSet setDiff = setDiff(other, accumulator);
				List<DataPoint> list = Stream.concat(accumulator.stream(), setDiff.stream()).collect(toList());
				accumulator = new LightDataSet(metadata, list::stream);
			}
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

		return metadata = (DataSetMetadata) meta.get(0);
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
		return operands.stream().map(Object::toString).collect(Collectors.joining(", ", setOperator + "(", ")"));
	}
}
