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

import static java.util.stream.Collectors.joining;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;

public class VTLMissingComponentsException extends VTLException
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public VTLMissingComponentsException(Collection<? extends DataStructureComponent<?, ?, ?>> structure, VTLAlias... missing)
	{
		super("Components " + Arrays.toString(missing) + " not found in " + structure);
	}

	public VTLMissingComponentsException(Set<? extends DataStructureComponent<?, ?, ?>> missing, Set<? extends DataStructureComponent<?, ?, ?>> structure)
	{
		super("Components " + missing.stream().map(c -> c.getVariable().toString()).collect(joining(", ", "[", "]")) + " not found in " + structure);
	}

	public VTLMissingComponentsException(Collection<? extends DataStructureComponent<?, ?, ?>> operand, DataStructureComponent<?, ?, ?> missing)
	{
		super("Component " + missing + " not found in " + operand);
	}

	public VTLMissingComponentsException(Map<? extends DataStructureComponent<?, ?, ?>, ? extends ScalarValue<?, ?, ?, ?>> datapoint, Set<? extends DataStructureComponent<?, ?, ?>> missing)
	{
		super("Component " + missing + " not found in " + datapoint);
	}
}
