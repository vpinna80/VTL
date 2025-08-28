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
package it.bancaditalia.oss.vtl.impl.environment.sampledata;

import static java.util.stream.Collectors.toSet;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;

import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class VTLSampleDataEnvironment implements Environment
{
	private static final Set<VTLAlias> ALIASES = Arrays.stream(SampleDataSets.values()).map(Object::toString).map(VTLAliasImpl::of).collect(toSet()); 
	
	@Override
	public Optional<VTLValue> getValue(MetadataRepository repo, VTLAlias alias)
	{
		return ALIASES.contains(alias) ? Optional.of(SampleDataSets.valueOf(alias.toString().toUpperCase())) : Optional.empty();
	}
}
