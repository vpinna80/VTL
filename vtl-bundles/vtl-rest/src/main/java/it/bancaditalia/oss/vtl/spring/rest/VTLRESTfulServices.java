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
package it.bancaditalia.oss.vtl.spring.rest;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.session.VTLSession;
import it.bancaditalia.oss.vtl.spring.rest.exception.VTLInvalidSessionException;
import it.bancaditalia.oss.vtl.spring.rest.result.ComponentBean;
import it.bancaditalia.oss.vtl.spring.rest.result.DataSetResultBean;
import it.bancaditalia.oss.vtl.spring.rest.result.DomainBean;
import it.bancaditalia.oss.vtl.spring.rest.result.ResultBean;
import it.bancaditalia.oss.vtl.spring.rest.result.ScalarResultBean;
import it.bancaditalia.oss.vtl.spring.rest.result.UUIDBean;

@RestController
@SpringBootApplication
@Configuration
public class VTLRESTfulServices extends SpringBootServletInitializer
{
	@Autowired private VTLSessionManager manager;
	
	@PostMapping(path = "/compile", params = "code")
	public @NonNull UUIDBean compile(@RequestParam @NonNull String code) 
	{
		UUID uuid = manager.createSession(code);
		VTLSession session = manager.getSession(uuid);
		session.compile();
		return new UUIDBean(uuid);
	}

	@GetMapping("/resolve")
	public @NonNull ResultBean resolve(@RequestParam @NonNull UUID uuid, @RequestParam @NonNull String alias) 
	{
		VTLSession session = manager.getSession(uuid);
		VTLValue value = session.resolve(alias);
		if (value instanceof ScalarValue)
			return new ScalarResultBean((ScalarValue<?, ?, ?, ?>) value);
		else
			return new DataSetResultBean((DataSet) value);
	}

	@GetMapping("/metadata")
	public @NonNull List<DomainBean> getMetadata(@RequestParam @NonNull UUID uuid, @RequestParam @NonNull String alias) 
	{
		if (!manager.containsSession(uuid))
			throw new VTLInvalidSessionException(uuid); 
			
		VTLSession session = manager.getSession(uuid);
		VTLValueMetadata value = session.getMetadata(alias);
		if (value instanceof ScalarValueMetadata)
			return singletonList(new DomainBean(((ScalarValueMetadata<?, ?>) value).getDomain()));
		else
			return ((DataSetMetadata) value).stream().map(ComponentBean::new).collect(toList());
	}
}
