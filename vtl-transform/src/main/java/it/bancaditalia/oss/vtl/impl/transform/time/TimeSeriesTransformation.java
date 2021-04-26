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
package it.bancaditalia.oss.vtl.impl.transform.time;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;

import java.util.Set;

import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireTimeDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.TimeDomain;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public abstract class TimeSeriesTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;

	public TimeSeriesTransformation(Transformation operand)
	{
		super(operand);
	}

	@Override
	protected final VTLValue evalOnScalar(ScalarValue<?, ?, ?, ?> scalar)
	{
		throw new VTLInvalidParameterException(scalar, DataSet.class);
	}
	
	@Override
	public final DataSetMetadata getMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata metadata = operand.getMetadata(scheme);
		
		if (!(metadata instanceof DataSetMetadata))
			throw new VTLInvalidParameterException(metadata, DataSetMetadata.class);
		
		DataSetMetadata ds = (DataSetMetadata) metadata;
		Set<DataStructureComponent<Identifier, EntireTimeDomainSubset, TimeDomain>> timeIDs = ds.getComponents(Identifier.class, TIMEDS);
		if (timeIDs.size() != 1)
			throw new VTLSingletonComponentRequiredException(Identifier.class, TIMEDS, ds);
		
		return checkIsTimeSeriesDataSet((DataSetMetadata) metadata, scheme);
	}

	protected abstract DataSetMetadata checkIsTimeSeriesDataSet(DataSetMetadata metadata, TransformationScheme scheme);

	@Override
	public int hashCode()
	{
		return super.hashCode();
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (!super.equals(obj)) return false;
		if (!(obj instanceof TimeSeriesTransformation)) return false;
		return true;
	}
}
