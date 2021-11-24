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
package it.bancaditalia.oss.vtl.impl.transform.dataset;

import static it.bancaditalia.oss.vtl.impl.types.data.BooleanValue.TRUE;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.transform.scope.DatapointScope;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageCall;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class FilterClauseTransformation extends DatasetClauseTransformation
{
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unused")
	private final static Logger LOGGER = LoggerFactory.getLogger(FilterClauseTransformation.class);

	private final Transformation filterClause;

	public FilterClauseTransformation(Transformation filterClause)
	{
		this.filterClause = filterClause;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return filterClause.getTerminals();
	}

	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		DataSet operand = (DataSet) getThisValue(scheme);
		final DataSetMetadata metadata = (DataSetMetadata) getMetadata(scheme);

		return operand.filter(dp -> {
			final DatapointScope dpScope = new DatapointScope(dp, metadata);
			return (TRUE.equals(BOOLEANDS.cast((ScalarValue<?, ?, ?, ?>) filterClause.eval(dpScope))));
		});
	}

	public VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata filterMetadata = filterClause.getMetadata(scheme);
		
		if (filterMetadata instanceof ScalarValueMetadata)
		{
			if (!BOOLEANDS.isAssignableFrom(((ScalarValueMetadata<?, ?>) filterMetadata).getDomain()))
				throw new VTLIncompatibleTypesException("FILTER", BOOLEANDS, ((ScalarValueMetadata<?, ?>) filterMetadata).getDomain());
		}
		else
		{
			DataSetMetadata filterDataset = (DataSetMetadata) filterMetadata;
			Set<DataStructureComponent<Measure, ?, ?>> measures = filterDataset.getComponents(Measure.class);
			if (measures.size() != 1)
				throw new VTLExpectedComponentException(Measure.class, BOOLEANDS, measures);
			
			DataStructureComponent<Measure, ?, ?> measure = measures.iterator().next();
			if (!BOOLEANDS.isAssignableFrom(measure.getDomain()))
				throw new VTLIncompatibleTypesException("FILTER", BOOLEANDS, measure.getDomain());
		}
			
		
		return getThisMetadata(scheme);
	}

	@Override
	public String toString()
	{
		return "filter " + filterClause;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((filterClause == null) ? 0 : filterClause.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (!(obj instanceof FilterClauseTransformation)) return false;
		FilterClauseTransformation other = (FilterClauseTransformation) obj;
		if (filterClause == null)
		{
			if (other.filterClause != null) return false;
		}
		else if (!filterClause.equals(other.filterClause)) return false;
		return true;
	}
	
	@Override
	protected Lineage computeLineage()
	{
		return LineageNode.of("filter", LineageCall.of(filterClause.getLineage()));
	}
}
