package com.heb.pm.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.heb.pm.dao.event.EventPublisher;
import com.heb.pm.dao.event.EventPublisherDelegate;
import com.heb.pm.dao.event.EventPublisherTimer;
import com.heb.pm.dao.event.JmsEventPublisherDelegate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

/**
 * Configures connection to the core (EMD) database.
 *
 * @author m314029
 * @since 1.2.0
 */
@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(basePackages = {"com.heb.pm.core.repository", "com.heb.pm.dao.oracle",
		"com.heb.pm.util.jpa", "com.heb.pm.dao.core.converters"})
@Profile({"local", "dev", "cert", "prod"})
public class CoreDbConfig {

	@Value("${app.core.jmsCorrelationId:test}")
	private transient String jmsPublicationCorrelationId;

	@Value("${app.core.eventPublicationMilliseconds:300000}")
	private transient Long eventPublicationTime;

	@Autowired
	private transient ObjectMapper objectMapper;

	/**
	 * Creates a bean for the connection to the token store.
	 *
	 * @return A bean for the connection to the token store.
	 */
	@Primary
	@Bean(name = "coreDataSource")
	@ConfigurationProperties(prefix = "core.datasource")
	public DataSource coreDataSource() {
		return DataSourceBuilder.create().build();
	}

	/**
	 * Creates a JDBC Template for the core DB connection.
	 *
	 * @return A JDBC Template for the core DB connection..
	 */
	@Bean(name = "jdbcTemplate")
	public JdbcTemplate jdbcTemplate() {
		return new JdbcTemplate(this.coreDataSource());
	}

	/**
	 * Creates a bean for the default entity manager.
	 *
	 * @return A bean for the default entity manager.
	 */
	@Primary
	@Bean(name = "entityManagerFactory")
	public LocalContainerEntityManagerFactoryBean entityManagerFactory(
			EntityManagerFactoryBuilder builder,
			@Qualifier("coreDataSource") DataSource dataSource) {
		return builder
				.dataSource(dataSource)
				.packages("com.heb.pm.dao.core")
				.persistenceUnit("coreEMF")
				.build();
	}

	/**
	 * Creates a bean for the default transaction manager.
	 *
	 * @return A bean for the default transaction manager.
	 */
	@Primary
	@Bean(name = "transactionManager")
	public JpaTransactionManager transactionManager(
			@Qualifier("entityManagerFactory") EntityManagerFactory entityManagerFactory) {
		return new JpaTransactionManager(entityManagerFactory);
	}

	/**
	 * Creates a bean that will do the DAO event publication.
	 *
	 * @return A bean that will do the DAO event publication.
	 */
	@Bean
	public EventPublisherDelegate jmsEventPublisherDelegate() {
		return new JmsEventPublisherDelegate(this.jmsPublicationCorrelationId, this.objectMapper);
	}

	/**
	 * Creates a bean to publish database events.

	 * @return A bean to publish database events.
	 */
	@Bean
	public EventPublisher eventPublisher() {

		EventPublisher eventPublisher = new EventPublisher();
		eventPublisher.addDelegate(jmsEventPublisherDelegate());
		return eventPublisher;
	}

	/**
	 * Creates a bean that will trigger the EventPublisher based on a timer.
	 *
	 * @return A bean that will trigger the EventPublisher based on a timer.
	 * @throws Exception
	 */
	@Bean
	public EventPublisherTimer eventPublisherTimer() throws Exception {

		return new EventPublisherTimer(this.eventPublicationTime, eventPublisher());
	}
}
