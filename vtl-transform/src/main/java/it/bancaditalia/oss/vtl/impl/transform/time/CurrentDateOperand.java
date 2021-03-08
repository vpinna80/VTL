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
package it.bancaditalia.oss.vtl.impl.transform.time;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DAYS;
import static java.time.LocalDate.now;

import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class CurrentDateOperand extends TransformationImpl implements LeafTransformation
{
	private static final long serialVersionUID = 1L;

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		return new DateValue(now());
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme scheme)
	{
		return DAYS;
	}

	@Override
	public String toString()
	{
		return "current_date()";
	}
	
	@Override
	public String getText()
	{
		return toString();
	}
}
