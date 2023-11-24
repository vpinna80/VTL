package it.bancaditalia.oss.vtl.impl.jupyter;

import static it.bancaditalia.oss.vtl.impl.jupyter.JupyterMessage.createStatusMessage;
import static it.bancaditalia.oss.vtl.impl.jupyter.VTLJupyterKernel.KernelStatus.BUSY;
import static it.bancaditalia.oss.vtl.impl.jupyter.VTLJupyterKernel.KernelStatus.IDLE;
import static it.bancaditalia.oss.vtl.impl.jupyter.VTLJupyterKernel.KernelStatus.STARTING;
import static it.bancaditalia.oss.vtl.impl.jupyter.ZMQUtils.hexBytes;
import static it.bancaditalia.oss.vtl.impl.jupyter.ZMQUtils.hexString;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.Predicate.not;
import static org.zeromq.SocketType.PUB;
import static org.zeromq.SocketType.REP;
import static org.zeromq.SocketType.ROUTER;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.InvalidParameterException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.json.JsonMapper;

public class VTLJupyterKernel
{
	public enum KernelStatus
	{
		STARTING, BUSY, IDLE
	}
	
	private static final JsonFactory FACTORY = JsonFactory.builder().build().setCodec(new JsonMapper());
	
	private enum JupyterSocketType
	{
		shell(ROUTER), iopub(PUB), stdin(ROUTER), control(ROUTER), hb(REP);

		private final SocketType type;

		private JupyterSocketType(SocketType type)
		{
			this.type = type;
		}
		
		public SocketType getType()
		{
			return type;
		}
	}

	private final ZContext context = new ZContext();
	private final Mac hmac;
	private final MessageChannel iopubChannel;
	private final AtomicBoolean interrupted = new AtomicBoolean();

	public VTLJupyterKernel(Map<String, Object> connInfo) throws IOException, InvalidKeyException, NoSuchAlgorithmException
	{
		Map<JupyterSocketType, MessageChannel> threads = new HashMap<>();
		
		String address = connInfo.get("transport") + "://" + connInfo.get("ip") + ":";
		for (JupyterSocketType type : JupyterSocketType.values())
		{
			Socket socket = context.createSocket(type.getType());
			if (!socket.bind(address + connInfo.get(type + "_port")))
				throw new IllegalStateException();

			threads.put(type, new MessageChannel(type, socket));
		}

		byte key[] = ((String) connInfo.get("key")).getBytes(ZMQ.CHARSET);
		String algorithm = ((String) connInfo.get("signature_scheme"));
		hmac = Mac.getInstance(algorithm.replaceAll("-", ""));
		hmac.init(new SecretKeySpec(key, algorithm));

		iopubChannel = threads.get(JupyterSocketType.iopub);
		iopubChannel.publish(createStatusMessage(STARTING));
		
		for (Entry<JupyterSocketType, MessageChannel> channel: threads.entrySet())
			new Thread(channel.getValue()).start();
	}

	public class MessageChannel implements Runnable
	{
		private final JupyterSocketType type;
		private final Socket socket;
		private final BlockingQueue<ZMsg> queue = new LinkedBlockingQueue<>();

		private MessageChannel(JupyterSocketType type, Socket socket)
		{
			this.type = type;
			this.socket = socket;
		}

		public void publish(JupyterMessage msg)
		{
			try
			{
				queue.add(msg.encode(hmac));
			}
			catch (IOException e)
			{
				e.printStackTrace();
				interrupt();
			}
		}

		public void run()
		{
			try
			{
				switch (type.getType())
				{
					case PUB: pub(); break; // iopub
					case REP: echo(); break; // hb
					case ROUTER: router(); break;
					default: throw new UnsupportedOperationException("SocketType: " + type.getType());
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
				interrupt();
			}
			finally
			{
				System.err.println("Thread " + type + " terminated.");
			}
		}

		private void router() throws IOException, InvalidKeyException, NoSuchAlgorithmException, InterruptedException
		{
			while (!interrupted.get() && !Thread.interrupted())
			{
				JupyterMessage request = unwrap(ZMsg.recvMsg(socket));
				iopubChannel.publish(createStatusMessage(BUSY));

				switch (request.getMessageType())
				{
					case "kernel_info_request": process(request, MessageReplies::fillKernelInfo); break;
					case "execute_request": process(request, MessageReplies::execute); break;
					case "interrupt_request": interrupt(); process(request, MessageReplies::interrupt); break;
					default: throw new InvalidParameterException("msg_type: " + request.getMessageType());
				}

				while (!iopubChannel.queue.isEmpty())
					Thread.sleep(100);

				while (!queue.isEmpty())
					queue.remove().send(socket);

				iopubChannel.publish(createStatusMessage(IDLE));
			}
		}

		private void echo() throws IOException
		{
			while (!interrupted.get() && !Thread.interrupted())
				socket.send(socket.recv());
		}

		private void pub() throws IOException
		{
			while (!interrupted.get() && !Thread.interrupted())
				try
				{
					ZMsg head = queue.poll(100, MILLISECONDS);
					if (head != null)
						head.send(socket);
				}
				catch (InterruptedException e)
				{
					interrupt();
				}
		}

		private void process(JupyterMessage request, BiFunction<MessageChannel, JupyterMessage, JupyterMessage> transformer) throws IOException, NoSuchAlgorithmException, InvalidKeyException
		{
			publish(transformer.apply(iopubChannel, request));
		}
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> process(byte[] frame) throws IOException
	{
		return FACTORY.createParser(new String(frame, ZMQ.CHARSET)).readValueAs(Map.class);
	}

	public void interrupt()
	{
		interrupted.set(true);
		Thread.getAllStackTraces().keySet().stream().filter(not(Thread::isDaemon)).forEach(Thread::interrupt);
	}

	private JupyterMessage unwrap(ZMsg msg) throws IOException
	{
		List<byte[]> frames = new ArrayList<>();
		for (ZFrame frame : msg)
			frames.add(frame.getData());
		
		byte[] session = frames.get(0);
		byte[] hash = hexBytes(new String(frames.get(2), ZMQ.CHARSET));
		frames = frames.subList(3, frames.size());

		byte[] expected;
		synchronized (hmac)
		{
			for (byte[] frame : frames)
				hmac.update(frame);

			expected = hmac.doFinal();
		}
		
		if (expected.length != hash.length)
			throw new IllegalStateException("Differing signature length: Expected " + expected.length + " but was " + hash.length);
		if (!Arrays.equals(expected, hash))
			throw new IllegalStateException("Invalid signature: expected " + hexString(expected) + " but was " + hexString(hash));

		Queue<Map<String, Object>> dicts = new LinkedList<>();
		for (byte[] frame : frames)
			dicts.add(process(frame));

		return new JupyterMessage(session, dicts);
	}
}
