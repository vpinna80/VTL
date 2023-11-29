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
package it.bancaditalia.oss.vtl.impl.transform.exceptions;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import java.util.Collection;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;

public class VTLIncompatibleStructuresException extends VTLException
{
	private static final long serialVersionUID = 1L;

	public VTLIncompatibleStructuresException(String description, Collection<? extends DataStructureComponent<?, ?, ?>> expected, Collection<? extends DataStructureComponent<?, ?, ?>> actual)
	{
		super(description + ": expected " + expected + ", but was " + actual);
	}

	public VTLIncompatibleStructuresException(String description, Collection<Collection<? extends DataStructureComponent<?, ?, ?>>> structures)
	{
		super(description + ": " + structures.stream()
			.map(str -> str.stream().sorted(DataStructureComponent::byNameAndRole).collect(toList()).toString())
			.map(s -> "                    " + s).collect(joining("\n", "\n", "")));
	}
}
