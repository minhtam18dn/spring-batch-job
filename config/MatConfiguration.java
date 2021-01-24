package com.heb.pm.config;

import com.heb.pm.core.service.mat.BloombergRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Constructs beans related to handling MAT data.
 *
 * @author d116773
 * @since 1.22.0
 */
@Configuration
public class MatConfiguration {


	/**
	 * Constructs a bean to run queries against the Bloomberg tables.
	 *
	 * @param jdbcTemplate The JdbcTemplate to use to run queries.
	 * @return A bean to run queries against the Bloomberg tables.
	 */
	@Bean
	public BloombergRepository bloombergRepository(JdbcTemplate jdbcTemplate) {

		return new BloombergRepository(jdbcTemplate);
	}
}
