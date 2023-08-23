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

import java.util.UUID;

import javax.servlet.http.HttpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.context.annotation.SessionScope;

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.session.VTLSession;

@Service
@SessionScope
public class VTLSessionManager
{
	private static final Logger LOGGER = LoggerFactory.getLogger(VTLSessionManager.class);

	@Autowired private HttpSession httpSession;

	public UUID createSession()
	{
		UUID uuid = UUID.randomUUID();
		httpSession.setAttribute(uuid.toString(), ConfigurationManager.getDefault().createSession());
		LOGGER.info("Created session with UUID {}", uuid);
		return uuid;
	}

	public VTLSession getSession(UUID uuid)
	{
		LOGGER.info("Requested session with UUID {}", uuid);
		return (VTLSession) httpSession.getAttribute(uuid.toString());
	}

	public boolean containsSession(UUID uuid)
	{
		LOGGER.info("Requested session with UUID {}", uuid);
		return httpSession.getAttribute(uuid.toString()) != null;
	}
}
