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
package it.bancaditalia.oss.vtl.impl.transform.dataset;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.transform.scope.DatapointScope;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLScalarValueMetadata;
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
	private VTLDataSetMetadata metadata;

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
	public VTLValue eval(TransformationScheme session)
	{
		DataSet operand = (DataSet) getThisValue(session);

		return operand.filter(dp -> (BOOLEANDS.cast((ScalarValue<?, ?, ?>) filterClause.eval(new DatapointScope(dp, metadata, session))).get()));
//		return operand.filteredMappedJoin(alias, operand.getDataStructure(), filterDS, (a, b) -> (Boolean) b.get(filterColumn).get(), (a, b) -> a);
	}

	@Override
	public VTLDataSetMetadata getMetadata(TransformationScheme session)
	{
		if (metadata != null)
			return metadata;
		
		metadata = getThisMetadata(session);
		VTLValueMetadata filterMetadata = filterClause.getMetadata(session);
		
		if (filterMetadata instanceof VTLScalarValueMetadata)
		{
			if (!BOOLEANDS.isAssignableFrom(((VTLScalarValueMetadata<?>) filterMetadata).getDomain()))
				throw new VTLIncompatibleTypesException("FILTER", BOOLEANDS, ((VTLScalarValueMetadata<?>) filterMetadata).getDomain());
		}
		else
		{
			VTLDataSetMetadata filterDataset = (VTLDataSetMetadata) filterMetadata;
			Set<DataStructureComponent<Measure, ?, ?>> measures = filterDataset.getComponents(Measure.class);
			if (measures.size() != 1)
				throw new VTLExpectedComponentException(Measure.class, BOOLEANDS, measures);
			
			DataStructureComponent<Measure, ?, ?> measure = measures.iterator().next();
			if (!BOOLEANDS.isAssignableFrom(measure.getDomain()))
				throw new VTLIncompatibleTypesException("FILTER", BOOLEANDS, measure.getDomain());
		}
			
		
		return metadata = getThisMetadata(session);
	}

	@Override
	public String toString()
	{
		return "[filter " + filterClause + "]";
	}
}
