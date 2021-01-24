package com.heb.pm.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * Configures connection to Arbaf.
 *
 * @author m314029
 * @since 1.2.0
 */
@Configuration
@Profile({"local", "dev", "cert", "prod"})
public class ArbafDbConfig {

	/**
	 * Creates a bean for the connection to ARBAF.
	 *
	 * @return A bean for the connection to ARBAF.
	 */
	@Bean(name = "arbafDataSource")
	@ConfigurationProperties(prefix = "arbaf.datasource")
	public DataSource arbafDataSource() {
		return DataSourceBuilder.create().build();
	}

	/**
	 * Creates a JdbcTemplate bean on the connection to the ARBAF database.
	 *
	 * @return A JdbcTemplate bean on the connection to the ARBAF database.
	 */
	@Bean(name = "arbafJdbcTemplate")
	public JdbcTemplate arbafJdbcTemplate() {
		return new JdbcTemplate(this.arbafDataSource());
	}
}
