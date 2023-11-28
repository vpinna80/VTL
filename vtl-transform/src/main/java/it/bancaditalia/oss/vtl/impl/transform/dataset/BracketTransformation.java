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

import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.UnknownValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class BracketTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	private final DatasetClauseTransformation clause;
	private final String componentName;
	
	public BracketTransformation(Transformation operand, DatasetClauseTransformation clause, String componentName)
	{
		super(operand);
		this.clause = clause;
		this.componentName = componentName == null ? null : componentName.matches("'.*'") ? componentName.replaceAll("'(.*)'", "$1") : componentName.toLowerCase();
	}

	@Override
	protected VTLValue evalOnDataset(DataSet dataset, VTLValueMetadata metadata)
	{
		if (clause != null)
			return clause.eval(new ThisScope(dataset));
		else
			return dataset.membership(componentName);
	}
	
	@Override
	protected VTLValue evalOnScalar(ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		throw new UnsupportedOperationException();
	}

	public VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata metadata = operand.getMetadata(scheme);
		
		if (metadata instanceof UnknownValueMetadata)
			return INSTANCE;
		
		if (!(metadata instanceof DataSetMetadata))
			throw new UnsupportedOperationException("Dataset expected as left operand of '[]' or '#' but found " + metadata);

		if (clause != null)
			return clause.getMetadata(new ThisScope((DataSetMetadata) metadata));
		else
			return ((DataSetMetadata) metadata).membership(componentName);
	}
	
	@Override
	public String toString()
	{
		return operand + (clause != null ? "[" + clause.toString() + "]": "") + (componentName != null ? "#" + componentName : "");
	}
}
