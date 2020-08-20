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

import static java.util.Collections.emptySet;

import java.util.Set;

import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope;
import it.bancaditalia.oss.vtl.model.data.DataSet.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public abstract class DatasetClauseTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;

	@Override
	public boolean isTerminal()
	{
		return false;
	}
	
	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return emptySet();
	}

	protected VTLValue getThisValue(TransformationScheme session)
	{
		return session.resolve(ThisScope.THIS);
	}

	protected VTLDataSetMetadata getThisMetadata(TransformationScheme session)
	{
		return (VTLDataSetMetadata) session.getMetadata(ThisScope.THIS);
	}
}
