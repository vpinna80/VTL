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
package it.bancaditalia.oss.vtl.model.data;

import java.util.List;
import java.util.Map;

import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.VTLValue.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

public interface Hierarchy extends VTLValue, VTLValueMetadata
{
	enum CheckMode
	{
		NON_NULL, NON_ZERO, PARTIAL_NULL, PARTIAL_ZERO, ALWAYS_NULL, ALWAYS_ZERO;
	}

	public interface RuleItem
	{
		public String getCodeItem();

		public Transformation getCondition();

		public ScalarValue<?, ? extends IntegerDomainSubset, IntegerDomain> getErrorLevel();

		public ScalarValue<?, ? extends StringDomainSubset, StringDomain> getErrorCode();

		public List<? extends SourceItem> getComponents();

		public boolean contains(ScalarValue<?, ? extends StringDomainSubset, StringDomain> item);

		public int getSign(ScalarValue<?, ? extends StringDomainSubset, StringDomain> item);

		public Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>> validate(DataStructureComponent<Measure, ?, ?> measure, CheckMode mode,
				Map<? extends ScalarValue<?, ?, ?>, ? extends ScalarValue<?, ?, ?>> values);
	}

	public interface SourceItem
	{
		public String getId();

		public boolean isWithRelation();

		public Transformation getCondition();
	}

	public String getName();

	public List<RuleItem> getRuleItems();

	public DataStructureComponent<?, ?, ?> selectComponent(DataStructure structure);
}
