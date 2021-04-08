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

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class ConstantOperand<R extends Comparable<?> & Serializable, T extends ScalarValue<?, ?, ?, ?>> extends TransformationImpl implements LeafTransformation
{
	private static final long serialVersionUID = 1L;

	private final T value;
	private final ScalarValueMetadata<?, ?> metadata;
	
	public ConstantOperand(T value)
	{
		this.value = value;
		metadata = (ScalarValueMetadata<?, ?>) value.getMetadata();
	}

	@Override
	public T eval(TransformationScheme session)
	{
		return value;
	}

	@Override
	public String getText()
	{
		return value == null ? "null" : value.toString();
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return new HashSet<>();
	}

	@Override
	public ScalarValueMetadata<?, ?> getMetadata(TransformationScheme scheme)
	{
		return metadata;
	}

	@Override
	public String toString()
	{
		return value.toString();
	}
}
