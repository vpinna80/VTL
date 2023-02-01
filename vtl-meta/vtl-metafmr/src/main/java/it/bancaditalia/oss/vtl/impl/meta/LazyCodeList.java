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
