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

import static it.bancaditalia.oss.vtl.impl.transform.ops.SetTransformation.SetOperator.UNION;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static java.util.stream.Collectors.joining;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.BiFunctionDataSet;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;
import it.bancaditalia.oss.vtl.util.SerFunction;

public class SetTransformation extends TransformationImpl
{
//	private final static Logger LOGGER = LoggerFactory.getLogger(SetTransformation.class);
	private static final long serialVersionUID = 1L;

	public enum SetOperator
	{
		UNION(null),
		INTERSECT((structure) -> (left, right) -> left.setDiff(left.setDiff(right))), 
		SETDIFF((structure) -> (left, right) -> left.setDiff(right)), 
		SYMDIFF((structure) -> (left, right) -> new BiFunctionDataSet<>(structure,  
				(l, r) -> Stream.concat(l.setDiff(r).stream(), r.setDiff(l).stream()), left, right));

		private final SerFunction<DataSetMetadata, SerBinaryOperator<DataSet>> reducer;

		SetOperator(SerFunction<DataSetMetadata, SerBinaryOperator<DataSet>> reducer)
		{
			this.reducer = reducer;
		}
		
		public SerBinaryOperator<DataSet> getReducer(DataSetMetadata metadata)
		{
			return reducer.apply(metadata);
		}
		
		@Override
		public String toString()
		{
			return name().toLowerCase();
		}
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
		if (setOperator != UNION)
		{
			DataSet left = (DataSet) operands.get(0).eval(scheme);
			DataSet right = (DataSet) operands.get(1).eval(scheme);
		
			return setOperator.getReducer(left.getMetadata()).apply(left, right);
		}
		// Special case for UNION as it has the largest memory requirements
		else
		{
			DataSet[] datasets = operands.stream().skip(1).map(op -> (DataSet) op.eval(scheme)).collect(toList()).toArray(new DataSet[0]);
			return ((DataSet) operands.get(0).eval(scheme)).union(datasets);
		}
	}

	@Override
	public DataSetMetadata getMetadata(TransformationScheme scheme)
	{
		List<VTLValueMetadata> meta = operands.stream()
				.map(t -> t.getMetadata(scheme))
				.collect(toList());
		
		if (meta.stream().filter(m -> !(m instanceof DataSetMetadata)).findAny().isPresent())
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
		return LineageNode.of(this, operands.stream().map(Transformation::getLineage).collect(toList()).toArray(new Lineage[0]));
	}
}
