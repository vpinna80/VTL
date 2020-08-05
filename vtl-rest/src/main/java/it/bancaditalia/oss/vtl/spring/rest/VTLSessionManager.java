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
