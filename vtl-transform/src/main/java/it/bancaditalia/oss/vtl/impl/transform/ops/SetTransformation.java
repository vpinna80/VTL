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
import static it.bancaditalia.oss.vtl.util.SerUnaryOperator.identity;
import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.StreamWrapperDataSet;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
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
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;
import it.bancaditalia.oss.vtl.util.SerFunction;
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;

public class SetTransformation extends TransformationImpl
{
//	private final static Logger LOGGER = LoggerFactory.getLogger(SetTransformation.class);
	private static final long serialVersionUID = 1L;

	public enum SetOperator
	{
		UNION(null), // Specific support because of multi-argument possibility
		INTERSECT(linOp -> (left, right) -> setDiff(linOp, left, setDiff(identity(), left, right))), 
		SETDIFF(linOp -> (left, right) -> setDiff(linOp, left, right)), 
		SYMDIFF(linOp -> (left, right) -> concat(setDiff(linOp, left, right), setDiff(linOp, right, left)));

		private final SerFunction<SerUnaryOperator<Lineage>, SerBinaryOperator<DataSet>> reducer;

		SetOperator(SerFunction<SerUnaryOperator<Lineage>, SerBinaryOperator<DataSet>> reducer)
		{
			this.reducer = reducer;
		}
		
		public SerBinaryOperator<DataSet> getReducer(SerUnaryOperator<Lineage> linOp)
		{
			return reducer.apply(linOp);
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
			
			return setOperator.getReducer(lin -> LineageNode.of(this, lin)).apply(left, right);
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
		
		if (allMetadata.stream().anyMatch(m -> !(m.isDataSet())))
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
	
	private static DataSet setDiff(SerUnaryOperator<Lineage> linOp, DataSet left, DataSet right)
	{
		Set<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>> index;
		try (Stream<DataPoint> stream = right.stream())
		{
			index = stream.map(dp -> dp.getValues(Identifier.class)).collect(toSet());
		}
		
		return left.filter(dp -> !index.contains(dp.getValues(Identifier.class)), linOp); 
	}
	
	private static DataSet concat(DataSet left, DataSet right)
	{
		return new StreamWrapperDataSet(left.getMetadata(), () -> Stream.concat(left.stream(), right.stream()));
	}
}
