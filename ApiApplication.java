package com.heb.pm;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.data.neo4j.repository.config.EnableNeo4jRepositories;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * The driver class that kicks off this application.
 */
@EnableSwagger2
@SpringBootApplication
@EnableNeo4jRepositories("com.heb.pm.hierarchy.repository")
public class ApiApplication extends SpringBootServletInitializer {

	/**
	 * The method called to kick off this application.
	 *
	 * @param args Command line arguments to the application.
	 */
	public static void main(String[] args) {
		SpringApplication.run(ApiApplication.class, args);
	}

	/**
	 * Called by the SpringFramework to kick off configuring this application.
	 *
	 * @param builder The SpringApplicationBuilder being used by Spring.
	 * @return The same SpringApplicationBuilder updated.
	 */
	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
		return builder.sources(ApiApplication.class);
	}
}
