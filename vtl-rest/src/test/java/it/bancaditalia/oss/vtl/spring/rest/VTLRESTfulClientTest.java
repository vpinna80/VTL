package it.bancaditalia.oss.vtl.spring.rest;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.CookieManager;
import java.net.HttpCookie;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@EnableAutoConfiguration
public class VTLRESTfulClientTest
{
	static {
		System.setProperty("vtl.r", "disable");
	}
	
	@LocalServerPort private int port;
	
	@Test
	public void test() throws Throwable 
	{
		CookieManager cookies = new CookieManager();
		
		String url = "http://localhost:" + port + "/compile";
		HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
		connection.setRequestMethod("POST");
		connection.setDoOutput(true);
		PrintWriter writer = new PrintWriter(new OutputStreamWriter(connection.getOutputStream(), UTF_8));
		writer.println("code=" + URLEncoder.encode("a := 1;", "UTF-8"));
		writer.close();
		
		UUID uuid;
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream(), UTF_8)))
		{
			String line = reader.readLine();
			uuid = UUID.fromString(line.substring(1, line.length() - 1));
			
			List<String> list = connection.getHeaderFields().get("Set-Cookie");
			if (list != null)
				list.stream().forEach(cookie -> cookies.getCookieStore().add(null, HttpCookie.parse(cookie).get(0)));
		}
		catch (IOException e)
		{
			try (BufferedReader ereader = new BufferedReader(new InputStreamReader(connection.getErrorStream(), UTF_8)))
			{
				StringWriter out = new StringWriter();
				PrintWriter ewriter = new PrintWriter(out);
				ereader.lines().forEach(ewriter::println);
				fail(out.toString());
				return;
			}
		}
		
		url = "http://localhost:" + port + "/metadata?alias=a&uuid=" + URLEncoder.encode(uuid.toString(), "utf8");
		connection = (HttpURLConnection) new URL(url).openConnection();
		if (cookies.getCookieStore().getCookies().size() > 0)
		    connection.setRequestProperty("Cookie", cookies.getCookieStore().getCookies().stream().map(HttpCookie::toString).collect(joining(", ")));    
		try (InputStream is = connection.getInputStream())
		{
			byte buffer[] = new byte[200];
			while (is.read(buffer) >= 0)
				;
		}
		catch (IOException e)
		{
			try (BufferedReader ereader = new BufferedReader(new InputStreamReader(connection.getErrorStream(), UTF_8)))
			{
				StringWriter out = new StringWriter();
				PrintWriter ewriter = new PrintWriter(out);
				ereader.lines().forEach(ewriter::println);
				fail(out.toString());
				return;
			}
		}

		url = "http://localhost:" + port + "/resolve?alias=a&uuid=" + URLEncoder.encode(uuid.toString(), "utf8");
		connection = (HttpURLConnection) new URL(url).openConnection();
		if (cookies.getCookieStore().getCookies().size() > 0)
		    connection.setRequestProperty("Cookie", cookies.getCookieStore().getCookies().stream().map(HttpCookie::toString).collect(joining(", ")));    
		try (InputStream is = connection.getInputStream())
		{
			byte buffer[] = new byte[200];
			while (is.read(buffer) >= 0)
				;
		}
		catch (IOException e)
		{
			try (BufferedReader ereader = new BufferedReader(new InputStreamReader(connection.getErrorStream(), UTF_8)))
			{
				StringWriter out = new StringWriter();
				PrintWriter ewriter = new PrintWriter(out);
				ereader.lines().forEach(ewriter::println);
				fail(out.toString());
				return;
			}
		}
	}
}
