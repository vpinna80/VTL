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
package it.bancaditalia.oss.vtl.impl.transform;

import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static it.bancaditalia.oss.vtl.util.SerUnaryOperator.identity;
import static java.util.Objects.requireNonNull;

import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class VarIDOperand implements LeafTransformation
{
	private static final long serialVersionUID = 1L;

	private final VTLAlias alias;
	
	public VarIDOperand(VTLAlias name)
	{
		this.alias = requireNonNull(name);
	}
	
	@Override
	public VTLValue eval(TransformationScheme session)
	{
		VTLValue vtlValue = session.resolve(alias);
		
		if (vtlValue.isDataSet())
		{
			DataSet dataset = (DataSet) vtlValue;
			vtlValue = dataset.mapKeepingKeys(dataset.getMetadata(), lineageEnricher(this), identity());
		}
		
		return vtlValue;
	}

	@Override
	public String getText()
	{
		return alias.toString();
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme scheme)
	{
		return scheme.getMetadata(alias);
	}
	
	@Override
	public String toString()
	{
		return alias.toString();
	}
}
