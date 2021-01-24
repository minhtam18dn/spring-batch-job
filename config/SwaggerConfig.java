package com.heb.pm.config;

import com.heb.pm.ApiApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * Configures Swagger Documentation for SpringBoot.
 * @author o7923891
 * @since 1.0.0
 */
@EnableSwagger2
@ComponentScan(basePackageClasses = ApiApplication.class)
@Configuration
public class SwaggerConfig {

	private static final String SWAGGER_API_VERSION = "1.0";
	private static final String LICENSE_TEXT = "License";
	private static final String TITLE = "PRODUCT REST API";
	private static final String DESCRIPTION = "RESTful API for Product";

	/**
	 * Returns ApiInfoBuilder.
	 *
	 * @return ApiInfoBuilder.
	 */
	private ApiInfo apiInfo() {
		return new ApiInfoBuilder()
				.title(TITLE)
				.description(DESCRIPTION)
				.license(LICENSE_TEXT)
				.version(SWAGGER_API_VERSION)
				.build();
	}

	/**
	 * Returns Docket.
	 *
	 * @return Docket.
	 */
	@Bean
	public Docket productsApi() {
		return new Docket(DocumentationType.SWAGGER_2)
				.apiInfo(apiInfo())
				.pathMapping("/")
				.select()
				.apis(RequestHandlerSelectors.any())
				.paths(PathSelectors.any())
				.build();
	}
}
