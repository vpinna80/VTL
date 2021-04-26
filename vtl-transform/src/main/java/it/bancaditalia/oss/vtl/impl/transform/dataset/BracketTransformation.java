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

import static it.bancaditalia.oss.vtl.model.data.UnknownValueMetadata.INSTANCE;

import java.util.HashSet;
import java.util.Set;

import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope;
import it.bancaditalia.oss.vtl.impl.transform.util.MetadataHolder;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.UnknownValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class BracketTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	private final Transformation operand;
	private final DatasetClauseTransformation clause;
	private final String componentName;
	
	public BracketTransformation(Transformation operand, DatasetClauseTransformation clause, String componentName)
	{
		this.operand = operand;
		this.clause = clause;
		this.componentName = componentName == null ? null : componentName.matches("'.*'") ? componentName.replaceAll("'(.*)'", "$1") : componentName.toLowerCase();
	}

	@Override
	public boolean isTerminal()
	{
		return false;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		Set<LeafTransformation> result = new HashSet<>(operand.getTerminals());

		if (clause != null) 
			result.addAll(clause.getTerminals());

		return result;
	}

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		if (clause != null)
			return clause.eval(new ThisScope((DataSet) operand.eval(session), session));
		else
			return ((DataSet) operand.eval(session)).membership(componentName);
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme scheme)
	{
		return (DataSetMetadata) MetadataHolder.getInstance(scheme).computeIfAbsent(this, t -> computeMetadata(scheme));
	}
	
	public VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata metadata = operand.getMetadata(scheme);
		
		if (metadata instanceof UnknownValueMetadata)
			return INSTANCE;
		
		if (!(metadata instanceof DataSetMetadata))
			throw new UnsupportedOperationException("Dataset expected as left operand of []# but found " + metadata);

		if (clause != null)
			return clause.getMetadata(new ThisScope((DataSetMetadata) operand.getMetadata(scheme)));
		else
			return ((DataSetMetadata) metadata).membership(componentName);
	}
	
	@Override
	public String toString()
	{
		return operand + (clause != null ? clause.toString() : "") + (componentName != null ? "#" + componentName : "");
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((clause == null) ? 0 : clause.hashCode());
		result = prime * result + ((componentName == null) ? 0 : componentName.hashCode());
		result = prime * result + ((operand == null) ? 0 : operand.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (!(obj instanceof BracketTransformation)) return false;
		BracketTransformation other = (BracketTransformation) obj;
		if (clause == null)
		{
			if (other.clause != null) return false;
		}
		else if (!clause.equals(other.clause)) return false;
		if (componentName == null)
		{
			if (other.componentName != null) return false;
		}
		else if (!componentName.equals(other.componentName)) return false;
		if (operand == null)
		{
			if (other.operand != null) return false;
		}
		else if (!operand.equals(other.operand)) return false;
		return true;
	}
}
