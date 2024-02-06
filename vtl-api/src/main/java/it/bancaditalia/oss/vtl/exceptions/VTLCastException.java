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
package it.bancaditalia.oss.vtl.exceptions;

import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public class VTLCastException extends VTLException
{
	private static final long serialVersionUID = 1L;

	public VTLCastException(ValueDomainSubset<?, ?> domain, ScalarValue<?, ?, ?, ?> value)
	{
		super("Cannot cast " + value + "[" + value.getDomain() + "] to type " + domain + ".");
	}

	public VTLCastException(DataStructureComponent<?, ?, ?> component, ScalarValue<?, ?, ?, ?> value)
	{
		super("Component " + component + " cannot accept a value of " + value);
	}

	public VTLCastException(ValueDomainSubset<?, ?> accepting, ValueDomainSubset<?, ?> providing)
	{
		super("Domain " + providing + " is different from " + accepting);
	}
}
