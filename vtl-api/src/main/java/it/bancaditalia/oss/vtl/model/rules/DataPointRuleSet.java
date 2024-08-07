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
package it.bancaditalia.oss.vtl.model.rules;

import java.util.List;
import java.util.Map.Entry;

import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public interface DataPointRuleSet extends RuleSet
{
	public interface DataPointRule
	{
		public Boolean eval(DataPoint dp, TransformationScheme scheme);

		public String getRuleId();

		public ScalarValue<?, ?, ?, ?> getErrorCode();

		public ScalarValue<?, ?, ?, ?> getErrorLevel();
	}
	
	public List<Entry<String, String>> getVars();

	public List<DataPointRule> getRules();
}
