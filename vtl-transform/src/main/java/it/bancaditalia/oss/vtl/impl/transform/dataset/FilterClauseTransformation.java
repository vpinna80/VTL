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
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.transform.scope.DatapointScope;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

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
		final DataSetStructure metadata = (DataSetStructure) getMetadata(scheme);

		MetadataRepository repo = scheme.getRepository();
		return operand.filter(dp -> {
			DatapointScope dpScope = new DatapointScope(repo, dp, metadata, null);
			ScalarValue<?, ?, ?, ?> filterValue = (ScalarValue<?, ?, ?, ?>) filterClause.eval(dpScope);
			return TRUE.equals(BOOLEANDS.cast(filterValue));
		}, lineageEnricher(this));
	}

	public VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata filterMetadata = filterClause.getMetadata(scheme);
		
		if (!filterMetadata.isDataSet())
		{
			if (!BOOLEANDS.isAssignableFrom(((ScalarValueMetadata<?, ?>) filterMetadata).getDomain()))
				throw new VTLIncompatibleTypesException("filter", BOOLEANDS, ((ScalarValueMetadata<?, ?>) filterMetadata).getDomain());
		}
		else
			((DataSetStructure) filterMetadata).getSingleton(Measure.class, BOOLEANDS);
		
		return getThisMetadata(scheme);
	}

	@Override
	public String toString()
	{
		return "filter " + filterClause;
	}
}
