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
import java.lang.ref.SoftReference;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public abstract class TransformationImpl implements Transformation, Serializable
{
	private static final long serialVersionUID = 1L;
	private final static Logger LOGGER = LoggerFactory.getLogger(TransformationImpl.class);
	
	private transient SoftReference<Lineage> lineageCache = new SoftReference<>(null);

	@Override
	public abstract int hashCode();
	
	@Override
	public abstract boolean equals(Object obj);
	
	@Override
	public final Lineage getLineage()
	{
		Lineage lineage = lineageCache == null ? null : lineageCache.get();
		if (lineage == null)
		{
			LOGGER.trace("Starting computing lineage for {}...", this);
			lineage = computeLineage(); 
			lineageCache = new SoftReference<>(lineage);
			LOGGER.trace("Finished computing lineage for {}.", this);
		}
		return lineage;
	}

	protected abstract Lineage computeLineage();
	
	@Override
	public final VTLValueMetadata getMetadata(TransformationScheme scheme)
	{
		Map<Transformation, VTLValueMetadata> holder = scheme.getResultHolder(VTLValueMetadata.class);
		VTLValueMetadata metadata = holder.get(this);
		if (metadata == null)
		{
			metadata = computeMetadata(scheme);
			holder.put(this, metadata);
		}
		
		return metadata;
	}

	protected abstract VTLValueMetadata computeMetadata(TransformationScheme scheme);
}
