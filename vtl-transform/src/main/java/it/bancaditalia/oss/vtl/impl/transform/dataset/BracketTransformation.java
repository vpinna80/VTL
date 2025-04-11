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

import static it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope.THIS;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static it.bancaditalia.oss.vtl.model.data.UnknownValueMetadata.INSTANCE;
import static java.util.Collections.emptySet;

import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.UnknownValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class BracketTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	
	private final DatasetClauseTransformation clause;
	private final VTLAlias componentName;
	private final Transformation operand;
	
	public BracketTransformation(Transformation operand, DatasetClauseTransformation clause, VTLAlias componentName)
	{
		this.operand = operand;
		this.clause = clause;
		this.componentName = componentName;
	}

	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		DataSet value = (DataSet) (operand == null ? scheme.resolve(THIS) : operand.eval(scheme));
		
		try
		{
			if (value.isDataSet())
				if (clause != null)
					return clause.eval(new ThisScope(scheme.getRepository(), value, scheme));
				else
					return value.membership(componentName, lineageEnricher(this));
			else
				throw new VTLInvalidParameterException(value, DataSet.class);
		}
		catch (VTLException e)
		{
			throw new VTLNestedException("In expression " + this, e);
		}
	}
	
	public VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata metadata = operand.getMetadata(scheme);
		
		if (metadata instanceof UnknownValueMetadata)
			return INSTANCE;
		
		if (!(metadata.isDataSet()))
			throw new VTLInvalidParameterException(metadata, DataSetMetadata.class);

		if (clause != null)
			return clause.getMetadata(new ThisScope(scheme.getRepository(), (DataSetMetadata) metadata, scheme));
		else
			return ((DataSetMetadata) metadata).membership(componentName);
	}
	
	@Override
	public String toString()
	{
		return operand + (clause != null ? "[" + clause.toString() + "]": "") + (componentName != null ? "#" + componentName : "");
	}

	
	@Override
	public final boolean isTerminal()
	{
		return operand != null;
	}
	
	@Override
	public final Set<LeafTransformation> getTerminals()
	{
		return operand != null ? operand.getTerminals() : emptySet();
	}
}
