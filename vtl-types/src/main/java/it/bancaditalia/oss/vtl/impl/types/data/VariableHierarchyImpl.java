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
package it.bancaditalia.oss.vtl.impl.types.data;

import java.util.List;
import java.util.Map;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.VariableHierarchy;

public class VariableHierarchyImpl extends HierarchyImpl implements VariableHierarchy
{
	private static final long serialVersionUID = 1L;

	public VariableHierarchyImpl(String item, List<? extends RuleItem> rules, Map<String, String> conditions)
	{
		super(item, rules, conditions);
	}

	@Override
	public DataStructureComponent<?, ?, ?> selectComponent(DataSetMetadata structure)
	{
		return structure.getComponent(getItem()).orElseThrow(() -> new VTLMissingComponentsException(getItem(), structure));
	}
}
