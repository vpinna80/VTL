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
package it.bancaditalia.oss.vtl.impl.meta;

import static java.util.stream.Collectors.toSet;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import io.sdmx.api.sdmx.model.beans.codelist.CodeBean;
import io.sdmx.api.sdmx.model.beans.codelist.CodelistBean;
import io.sdmx.api.sdmx.model.beans.reference.StructureReferenceBean;
import io.sdmx.core.sdmx.manager.structure.SdmxRestToBeanRetrievalManager;
import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.impl.meta.subsets.AbstractStringCodeList;
import it.bancaditalia.oss.vtl.impl.meta.subsets.StringCodeList;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.util.Utils;

public class LazyCodeList extends AbstractStringCodeList implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final StructureReferenceBean clRef;
	private transient final Set<StringCodeItemImpl> cache = new HashSet<>();

	public LazyCodeList(StringDomainSubset<?> parent, StructureReferenceBean clRef)
	{
		super(parent, clRef.getAgencyId() + ":" + clRef.getMaintainableId() + "(" + clRef.getVersion() + ")", s -> new StringCodeList(parent, clRef.getAgencyId() + ":" + clRef.getMaintainableId() + "(" + clRef.getVersion() + ")", s));
		this.clRef = clRef;
	}

	@Override
	public Set<StringCodeItemImpl> getCodeItems()
	{
		if (cache.isEmpty())
			synchronized (this)
			{
				if (cache.isEmpty())
				{
					SdmxRestToBeanRetrievalManager rbrm = ((FMRRepository) ConfigurationManagerFactory.getInstance().getMetadataRepository()).getBeanRetrievalManager();
					cache.addAll(Utils.getStream(rbrm.getIdentifiableBean(clRef, CodelistBean.class).getRootCodes())
							.map(CodeBean::getId)
							.map(StringCodeItemImpl::new)
							.collect(toSet()));
				}
			}
		
		return cache;
	}
}
