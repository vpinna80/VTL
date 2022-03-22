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

import java.util.ArrayList;
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
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;

public class SetTransformation extends TransformationImpl
{
//	private final static Logger LOGGER = LoggerFactory.getLogger(SetTransformation.class);
	private static final long serialVersionUID = 1L;

	public enum SetOperator
	{
		UNION(null), // Particular support because of multi-argument possibility
		INTERSECT(structure -> lineageOp -> (left, right) -> left.setDiff(left.setDiff(right))), 
		SETDIFF(structure -> lineageOp -> (left, right) -> left.setDiff(right)), 
		SYMDIFF(structure -> enricher -> (left, right) -> new BiFunctionDataSet<>(structure,  
				(l, r) -> Stream.concat(l.setDiff(r).stream(), r.setDiff(l).stream())
					.map(dp -> dp.enrichLineage(enricher)), left, right));

		private final SerFunction<DataSetMetadata, SerFunction<SerUnaryOperator<Lineage>, SerBinaryOperator<DataSet>>> reducer;

		SetOperator(SerFunction<DataSetMetadata, SerFunction<SerUnaryOperator<Lineage>, SerBinaryOperator<DataSet>>> reducer)
		{
			this.reducer = reducer;
		}
		
		public SerFunction<SerUnaryOperator<Lineage>,SerBinaryOperator<DataSet>> getReducer(DataSetMetadata metadata)
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
		
			return setOperator.getReducer(left.getMetadata()).apply(l -> LineageNode.of(this, l)).apply(left, right);
		}
		// Special case for UNION as it has the largest memory requirements
		else
		{
			List<DataSet> datasets = new ArrayList<>();
			boolean first = true;
			for (Transformation operand: operands)
				if (!first)
					datasets.add((DataSet) operand.eval(scheme));
				else
					first = false;
			return ((DataSet) operands.get(0).eval(scheme)).union(dp -> LineageNode.of(this, dp.getLineage()), datasets);
		}
	}

	@Override
	protected DataSetMetadata computeMetadata(TransformationScheme scheme)
	{
		List<VTLValueMetadata> allMetadata = operands.stream()
				.map(t -> t.getMetadata(scheme))
				.collect(toList());
		
		if (allMetadata.stream().anyMatch(m -> !(m instanceof DataSetMetadata)))
			throw new UnsupportedOperationException("In set operation expected all datasets but found a scalar"); 
			
		if (allMetadata.stream().distinct().limit(2).count() != 1)
			throw new UnsupportedOperationException("In set operation expected all datasets with equal structure but found: " + allMetadata); 

		return (DataSetMetadata) allMetadata.get(0);
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
	public Lineage computeLineage()
	{
		return LineageNode.of(this, operands.stream().map(Transformation::getLineage).collect(toList()).toArray(new Lineage[0]));
	}
}
