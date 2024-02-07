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
package it.bancaditalia.oss.vtl.impl.jupyter;

import static it.bancaditalia.oss.vtl.impl.jupyter.JupyterMessage.createReplyMessage;
import static it.bancaditalia.oss.vtl.impl.jupyter.JupyterMessage.createStreamMessage;
import static it.bancaditalia.oss.vtl.util.Utils.entryByValue;
import static it.bancaditalia.oss.vtl.util.Utils.keepingKey;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;

import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.impl.jupyter.VTLJupyterKernel.MessageChannel;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Role;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.session.VTLSession;

public class MessageReplies
{
	private static final Map<String, VTLSession> SESSIONS = new ConcurrentHashMap<>();
	private static final TemplateEngine THYMELEAF = new TemplateEngine();
	private static final Map<Class<? extends Component>, String> ROLES = new HashMap<>(); 
//	private static final JsonFactory FACTORY = JsonFactory.builder().build().setCodec(new JsonMapper());
	
	static
	{
		THYMELEAF.setTemplateResolver(new ClassLoaderTemplateResolver(MessageReplies.class.getClassLoader()));
		for (Role role: Role.values())
			ROLES.put(role.roleClass(), role.roleClass().getSimpleName());
	}
	
	public static JupyterMessage fillKernelInfo(MessageChannel iopub, JupyterMessage request)
	{
		JupyterMessage reply = createReplyMessage("reply", request);
		
		Map<String, Object> languageInfo = new HashMap<>();
		languageInfo.put("name", "VTL");
		languageInfo.put("version", "2.0");
		languageInfo.put("mimetype", "text");
		languageInfo.put("file_extension", "vtl");
	
		Map<String, Object> content = reply.getContent();
		content.put("status", "ok");
		content.put("protocol_version", "5.3");
		content.put("implementation", "VTL E&E");
		content.put("implementation_version", VTLKernelLauncher.class.getPackage().getImplementationVersion());
		content.put("banner", "****** VTL E&E Kernel ******");
		content.put("language_info", languageInfo);
		
		return reply;
	}

	public static JupyterMessage interrupt(MessageChannel iopub, JupyterMessage response)
	{
		response.getContent().put("status", "ok");
		return response;
	}
	
	public static JupyterMessage execute(MessageChannel iopub, JupyterMessage request)
	{
		JupyterMessage reply = createReplyMessage("execute_reply", request);
		reply.getContent().put("status", "ok");

		String code = request.getContent().get("code").toString();
		int count = request.getContent().get("store_history") != FALSE ? reply.addExecutionCount() : -1;
		
		if (request.getContent().get("silent") != TRUE)
		{
			JupyterMessage input = createReplyMessage("execute_input", request);
			input.getContent().put("execution_count", count);
			input.getContent().put("code", code);
			iopub.publish(input);
		}
		
		try
		{
			switch (code.trim().split("\\s", 2)[0])
			{
				case "%eval": eval(iopub, request, reply, code, count); break;
				default: compile(iopub, request, reply, code, count); break;
			}
		}
		catch (Exception e)
		{
			if (request.getContent().get("silent") != TRUE)
			{
				StringWriter writer = new StringWriter();
				e.printStackTrace(new PrintWriter(writer));
				iopub.publish(createStreamMessage("stderr", writer.toString(), request));
			}
		}
		
		return reply;
	}

	private static void eval(MessageChannel iopub, JupyterMessage request, JupyterMessage reply, String code, int count)
	{
		if (request.getContent().get("silent") != TRUE)
		{
			VTLSession vtlSession = SESSIONS.get(reply.getParentHeader().get("session").toString());
	
			String[] tokens = code.split("\\s+");
			AtomicInteger start = new AtomicInteger(1);
			AtomicInteger dpCount = new AtomicInteger(100);
			try
			{
				dpCount.set(Integer.parseInt(tokens[1]));
				start.set(2);
			}
			catch (NumberFormatException e)
			{
			}
			
			Map<String, VTLValue> values = new HashMap<>();
			for (int i = start.get(); i < tokens.length; i++)
				values.put(tokens[i], vtlSession.resolve(tokens[i]));
			
			Map<Boolean, List<Entry<String, VTLValue>>> partitioned = values.entrySet().stream().collect(partitioningBy(entryByValue(ScalarValue.class::isInstance)));
			JupyterMessage results = createReplyMessage("execute_result", request);
			results.getContent().put("execution_count", count);
			results.getContent().put("metadata", "{}");

			Map<String, Object> data = new HashMap<>();
			results.getContent().put("data", data);
			
			Map<String, Object> templateVars = new HashMap<>();
			templateVars.put("count", count); 
			templateVars.put("roles", ROLES); 
			templateVars.put("scalars", partitioned.get(TRUE)); 
			templateVars.put("datasets", partitioned.get(FALSE).stream()
					.map(keepingKey(ds -> {
						try (Stream<DataPoint> stream = ((DataSet) ds).stream())
						{
							return new SimpleEntry<>(ds.getMetadata(), stream.limit(dpCount.get()).collect(toList()));
						}
					})).collect(toList()));
			data.put("text/html", THYMELEAF.process("it/bancaditalia/oss/vtl/impl/jupyter/datasets.html", new Context(Locale.getDefault(), templateVars)));

			iopub.publish(results);
		}
	}

	private static void compile(MessageChannel iopub, JupyterMessage request, JupyterMessage reply, String code, int count)
	{
		String sessionName = reply.getParentHeader().get("session").toString();
		VTLSession oldSession = SESSIONS.get(sessionName);
		VTLSession vtlSession = SESSIONS.computeIfAbsent(sessionName, s -> ConfigurationManagerFactory.getInstance().createSession(code));
		
		Map<Statement, VTLValueMetadata> compiled = new HashMap<>(vtlSession.compile());
		compiled.keySet().removeAll(oldSession.getWorkspace().getRules());
		
		Map<Boolean, List<Entry<Statement, VTLValueMetadata>>> partitioned = compiled.entrySet().stream().collect(partitioningBy(entryByValue(ScalarValueMetadata.class::isInstance)));
		
		if (request.getContent().get("silent") != TRUE)
		{
			JupyterMessage results = createReplyMessage("execute_result", request);
			results.getContent().put("execution_count", count);
			results.getContent().put("metadata", "{}");

			Map<String, Object> data = new HashMap<>();
			results.getContent().put("data", data);
			
			Map<String, Object> templateVars = new HashMap<>();
			templateVars.put("count", count); 
			templateVars.put("roles", ROLES); 
			templateVars.put("scalars", partitioned.get(TRUE)); 
			templateVars.put("structures", partitioned.get(FALSE));
			data.put("text/html", THYMELEAF.process("it/bancaditalia/oss/vtl/impl/jupyter/structures.html", new Context(Locale.getDefault(), templateVars)));

			iopub.publish(results);
		}
	}
}
