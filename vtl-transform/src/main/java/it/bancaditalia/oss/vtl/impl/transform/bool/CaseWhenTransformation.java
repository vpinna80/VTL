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
package it.bancaditalia.oss.vtl.impl.transform.bool;

import java.security.InvalidParameterException;
import java.util.List;
import java.util.Set;

import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class CaseWhenTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;

	private final ConditionalTransformation firstCondition;
	
	public CaseWhenTransformation(List<Transformation> conditions, List<Transformation> thenExprs, Transformation elseExpr)
	{
		if (conditions.size() != thenExprs.size() || conditions.size() <= 0)
			throw new InvalidParameterException();
		
		ConditionalTransformation c = null;
		for (int i = conditions.size() - 1; i >= 0; i--)
			if (c == null)
				c = new ConditionalTransformation(conditions.get(i), thenExprs.get(i), elseExpr);
			else
				c = new ConditionalTransformation(conditions.get(i), thenExprs.get(i), c);
		
		firstCondition = c;
	}

	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		return firstCondition.eval(scheme);
	}

	@Override
	public boolean hasAnalytic()
	{
		return firstCondition.hasAnalytic();
	}

	@Override
	public boolean isTerminal()
	{
		return false;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return firstCondition.getTerminals();
	}
	
	public VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		return firstCondition.getMetadata(scheme);
	}
	
	@Override
	public String toString()
	{
		return firstCondition.toString();
	}
}
