package it.bancaditalia.oss.vtl.spring.rest;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSet.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue.VTLScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue.VTLValueMetadata;
import it.bancaditalia.oss.vtl.session.VTLSession;
import it.bancaditalia.oss.vtl.spring.rest.exception.VTLInvalidSessionException;
import it.bancaditalia.oss.vtl.spring.rest.result.ComponentBean;
import it.bancaditalia.oss.vtl.spring.rest.result.DataSetResultBean;
import it.bancaditalia.oss.vtl.spring.rest.result.ResultBean;
import it.bancaditalia.oss.vtl.spring.rest.result.ScalarResultBean;

@RestController
@SpringBootApplication
@Configuration
public class VTLRESTfulServices extends SpringBootServletInitializer
{
	@Autowired private VTLSessionManager manager;
	
	@PostMapping(path = "/compile", params = "code")
	public UUID compile(@RequestParam String code) 
	{
		UUID uuid = manager.createSession();
		VTLSession session = manager.getSession(uuid);
		session.addStatements(code);
		session.compile();
		return uuid;
	}

	@GetMapping("/resolve")
	public ResultBean resolve(@RequestParam UUID uuid, @RequestParam String alias) 
	{
		VTLSession session = manager.getSession(uuid);
		VTLValue value = session.resolve(alias);
		if (value instanceof ScalarValue)
			return new ScalarResultBean((ScalarValue<?, ?, ?>) value);
		else
			return new DataSetResultBean((DataSet) value);
	}

	@GetMapping("/metadata")
	public Object getMetadata(@RequestParam UUID uuid, @RequestParam String alias) 
	{
		if (!manager.containsSession(uuid))
			throw new VTLInvalidSessionException(uuid); 
			
		VTLSession session = manager.getSession(uuid);
		VTLValueMetadata value = session.getMetadata(alias);
		if (value instanceof VTLScalarValueMetadata)
			return singletonMap("domain", ((VTLScalarValueMetadata<?>) value).getDomain().toString());
		else
			return ((VTLDataSetMetadata) value).stream().map(ComponentBean::new).collect(toList());
	}
}
