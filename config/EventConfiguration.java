package com.heb.pm.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.context.event.SimpleApplicationEventMulticaster;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

/**
 * Configuration for internal event handling.
 *
 * @author d116773
 * @since 1.25.0
 */
@Configuration
@Profile("!test")
public class EventConfiguration {

	private static final Logger logger = LoggerFactory.getLogger(EventConfiguration.class);

	/**
	 * Creates a ApplicationEventMulticaster bean. This will allow event listeners to run
	 * in a different thread than the code that issued the event.
	 *
	 * @return A ApplicationEventMulticaster bean.
	 */
	@Bean(name = "applicationEventMulticaster")
	public ApplicationEventMulticaster simpleApplicationEventMulticaster() {

		logger.info("Constructing asynchronous event listener.");

		SimpleApplicationEventMulticaster eventMulticaster =
				new SimpleApplicationEventMulticaster();

		eventMulticaster.setTaskExecutor(new SimpleAsyncTaskExecutor());
		return eventMulticaster;
	}
}
