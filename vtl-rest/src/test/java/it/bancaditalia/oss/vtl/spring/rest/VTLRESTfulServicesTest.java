package it.bancaditalia.oss.vtl.spring.rest;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;
import static org.springframework.http.HttpStatus.OK;
import static org.springframework.restdocs.headers.HeaderDocumentation.headerWithName;
import static org.springframework.restdocs.headers.HeaderDocumentation.requestHeaders;
import static org.springframework.restdocs.headers.HeaderDocumentation.responseHeaders;
import static org.springframework.restdocs.payload.PayloadDocumentation.fieldWithPath;
import static org.springframework.restdocs.payload.PayloadDocumentation.responseFields;
import static org.springframework.restdocs.request.RequestDocumentation.parameterWithName;
import static org.springframework.restdocs.request.RequestDocumentation.requestParameters;
import static org.springframework.restdocs.restassured3.RestAssuredRestDocumentation.document;
import static org.springframework.restdocs.restassured3.RestAssuredRestDocumentation.documentationConfiguration;

import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.restdocs.RestDocumentationContextProvider;
import org.springframework.restdocs.RestDocumentationExtension;
import org.springframework.restdocs.payload.JsonFieldType;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import io.restassured.builder.RequestSpecBuilder;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import it.bancaditalia.oss.vtl.spring.rest.result.UUIDBean;

@ExtendWith({RestDocumentationExtension.class, SpringExtension.class})
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@EnableAutoConfiguration
@TestMethodOrder(OrderAnnotation.class)
public class VTLRESTfulServicesTest
{
	static {
		System.setProperty("vtl.r", "disable");
	}

	private RequestSpecification documentationSpec;

	@BeforeEach
	public void setUp(RestDocumentationContextProvider restDocumentation) {
		this.documentationSpec = new RequestSpecBuilder()
				.addFilter(documentationConfiguration(restDocumentation)) 
				.build();
	}
	
	private static String jSessionID;
	private static UUID uuid;
	
	@LocalServerPort private int port;

	@Order(1)
	@Test
	public void testCompile() throws Throwable
	{
		ExtractableResponse<Response> postResponse = given(documentationSpec)
				.urlEncodingEnabled(true)
				.formParam("code", "a := 1; b := 2; c := a + b;")
				.port(port)
				.filter(document("compile",
					requestParameters(
						parameterWithName("code").description("The VTL code snippet to compile")
					), responseFields(
						fieldWithPath("uuid").description("The UUID of the session being created").type(UUID.class)
					), responseHeaders(
						headerWithName("JSESSIONID").description("The Java Servlet server session cookie.").optional()
					)
				))
			.when()
				.post("/compile")
			.then().assertThat()
					.statusCode(OK.value())
				.and()
					.body("uuid", matchesPattern("(?i)\\p{XDigit}{8}-\\p{XDigit}{4}-\\p{XDigit}{4}-\\p{XDigit}{4}-\\p{XDigit}{12}"))
				.and()
					.cookie("JSESSIONID")
			.extract();
		
		jSessionID = postResponse.cookie("JSESSIONID");
		uuid = postResponse.as(UUIDBean.class).getUuid();
	}
	
	@Order(2)
	@Test
	public void testMetadata() throws Throwable
	{
		given(documentationSpec)
			.param("alias", "c")
			.param("uuid", uuid)
			.cookie("JSESSIONID", jSessionID)
			.port(port)
			.accept("application/json")
			.filter(document("metadata", 
				requestParameters(
					parameterWithName("uuid").description("The UUID of the VTL session"),
					parameterWithName("alias").description("The alias of the object whose value is returned")
				), requestHeaders(
					headerWithName("JSESSIONID").description("The Java Servlet server session cookie.").optional()
				), responseFields(
					fieldWithPath("[].domain").description("The VTL domain of a scalar value or of a component of a dataset"),
					fieldWithPath("[].name").description("If the object is a dataset, the name of the component").type(JsonFieldType.STRING).optional(),
					fieldWithPath("[].role").description("If the object is a dataset, the role (IDENTIFIER, MEASURE, ATTRIBUTE) of the component").type(JsonFieldType.STRING).optional()
				)
			))
		.when()
			.get("/metadata")
		.then().assertThat()
				.statusCode(OK.value())
			.and()
				.body("[0].domain", equalTo("integer"));
	}

	@Order(3)
	@Test
	public void testResolve() throws Throwable
	{
		given(documentationSpec)
			.param("alias", "c")
			.param("uuid", uuid)
			.cookie("JSESSIONID", jSessionID)
			.port(port)
			.accept("application/json")
			.filter(document("resolve", 
				requestParameters(
					parameterWithName("uuid").description("The unique identifier of the VTL session"),
					parameterWithName("alias").description("The alias of the object whose value is returned")
				), requestHeaders(
					headerWithName("JSESSIONID").description("The Java Servlet server session cookie.").optional()
				), responseFields(
					fieldWithPath("type").description("Always SCALAR for scalar values"),
					fieldWithPath("domain").description("The VTL domain of the scalar value"),
					fieldWithPath("value").description("The computed scalar value")
				)
			))
		.when()
			.get("/resolve")
		.then().assertThat()
				.statusCode(OK.value())
			.and()
				.body("type", equalTo("SCALAR"))
			.and()
				.body("domain", equalTo("integer"))
			.and()
				.body("value", equalTo(3));
	}
}
