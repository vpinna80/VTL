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

import static it.bancaditalia.oss.vtl.impl.jupyter.ZMQUtils.hexString;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.crypto.Mac;

import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.json.JsonMapper;

import it.bancaditalia.oss.vtl.impl.jupyter.VTLJupyterKernel.KernelStatus;

public class JupyterMessage implements Serializable
{
	private static final JsonFactory FACTORY = JsonFactory.builder().build().setCodec(new JsonMapper());
	private static final long serialVersionUID = 1L;
	private static final UUID KERNEL_UUID = UUID.randomUUID();
	private static final byte[] KERNEL_UUID_BYTES;
	private static final Map<String, AtomicInteger> COUNTS = new ConcurrentHashMap<>(); 

	private final byte[] session;
	private final Map<String, Object> header = new HashMap<>();
	private final Map<String, Object> parentHeader = new HashMap<>();
	private final Map<String, Object> metadata = new HashMap<>();
	private final Map<String, Object> content = new HashMap<>();
	
	static
	{
		ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
	    bb.putLong(KERNEL_UUID.getMostSignificantBits());
	    bb.putLong(KERNEL_UUID.getLeastSignificantBits());
	    KERNEL_UUID_BYTES = bb.array();
	}

	public static JupyterMessage createStreamMessage(String name, String content, JupyterMessage request)
	{
		JupyterMessage msg = new JupyterMessage("stream", request);

		msg.content.put("name", name);
		msg.content.put("text", content);

		return msg;
	}

	public static JupyterMessage createStatusMessage(KernelStatus message)
	{
		JupyterMessage msg = new JupyterMessage("status");
		
		msg.content.put("execution_state", message.toString().toLowerCase());
		
		return msg;
	}
	
	public static JupyterMessage createReplyMessage(String messageType, JupyterMessage request)
	{
		return new JupyterMessage(messageType, request);
	}

	public JupyterMessage(byte[] session, Queue<Map<String, Object>> dicts)
	{
		this.session = session;
		
		for (Map<String, Object> dict: List.of(header, parentHeader, metadata, content))
			if (!dicts.isEmpty())
				dict.putAll(dicts.remove());
	}

	private JupyterMessage(String messageType)
	{
		session = KERNEL_UUID_BYTES;
				
		header.put("msg_id", UUID.randomUUID().toString().replaceAll("-", ""));
		header.put("session", KERNEL_UUID.toString().replaceAll("-", ""));
		header.put("username", System.getProperty("user.name"));
		header.put("date", Instant.now().toString());
		header.put("version", "5.2");
		header.put("msg_type", messageType);
	}
	
	private JupyterMessage(String messageType, JupyterMessage request)
	{
		this.session = request.session;
		
		header.put("msg_id", UUID.randomUUID().toString().replaceAll("-", ""));
		header.put("session", request.getHeader().get("session"));
		header.put("username", request.getHeader().get("username"));
		header.put("date", Instant.now().toString());
		header.put("msg_type", messageType);
		header.put("version", "5.0");
		
		parentHeader.putAll(request.getHeader());
	}
	
	public ZMsg encode(Mac hmac) throws IOException
	{
		List<byte[]> encoded = new ArrayList<>();
		encoded.add(getSession());
		encoded.add("<IDS|MSG>".getBytes(ZMQ.CHARSET));

		byte[] hash;
		synchronized (hmac)
		{
			for (Map<String, Object> dict: List.of(header, parentHeader, metadata, content))
			{
				StringWriter writer = new StringWriter();
				JsonGenerator generator = FACTORY.createGenerator(writer);
				generator.writeObject(dict);
				generator.close();
				byte[] frame = writer.toString().getBytes(ZMQ.CHARSET);
				encoded.add(frame);
				hmac.update(frame);
			}

			hash = hmac.doFinal();
		}

		encoded.add(2, hexString(hash).getBytes(ZMQ.CHARSET));
		
		ZMsg reply = new ZMsg();
		for (byte[] frame: encoded)
			reply.add(frame);
		return reply;
	}

	public String getMessageType()
	{
		return header.get("msg_type").toString(); 
	}

	public byte[] getSession()
	{
		return session;
	}
	
	public Map<String, Object> getHeader()
	{
		return header;
	}

	public Map<String, Object> getParentHeader()
	{
		return parentHeader;
	}

	public Map<String, Object> getMetadata()
	{
		return metadata;
	}

	public Map<String, Object> getContent()
	{
		return content;
	}
	
	@Override
	public String toString()
	{
		try
		{
			StringWriter writer = new StringWriter();
			JsonGenerator generator = FACTORY.createGenerator(writer).useDefaultPrettyPrinter();
			Map<String, Object> msg = new HashMap<>();
			msg.put("header", header);
			msg.put("parentHeader", parentHeader);
			msg.put("metadata", metadata);
			msg.put("content", content);
			generator.writeObject(msg);
			generator.close();
			return writer.toString();
		}
		catch (IOException e)
		{
			throw new UncheckedIOException(e);
		}
	}

	public int addExecutionCount()
	{
		int count = COUNTS.computeIfAbsent(getParentHeader().get("session").toString(), s -> new AtomicInteger(1)).getAndIncrement();
		content.put("execution_count", count);
		
		return count;
	}
}
