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
package it.bancaditalia.oss.vtl.impl.types.operators;

import static it.bancaditalia.oss.vtl.impl.types.data.BooleanValue.TRUE;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN;

import java.util.Set;

import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.SerPredicate;

public abstract class CriterionTransformation implements Transformation, SerPredicate<ScalarValue<?, ?, ?, ?>>
{
	private static final long serialVersionUID = 1L;

	@Override
	public final VTLValue eval(TransformationScheme scheme)
	{
		VTLValue value = scheme.resolve("$$THIS");
		
		if (value instanceof NullValue)
			return TRUE;
		else if (value instanceof ScalarValue)
			return BooleanValue.of(test((ScalarValue<?, ?, ?, ?>) value));
		else
			throw new UnsupportedOperationException("Cannot apply a domain criterion to a dataset");
	}

	@Override
	public final VTLValueMetadata getMetadata(TransformationScheme scheme)
	{
		return BOOLEAN;
	}

	@Override
	public Lineage getLineage()
	{
		throw new UnsupportedOperationException();
	}
	
	@Override
	public boolean isTerminal()
	{
		throw new UnsupportedOperationException();
	}
	
	@Override
	public Set<LeafTransformation> getTerminals()
	{
		throw new UnsupportedOperationException();
	}
}
