package com.heb.pm.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.util.HashMap;

/**
 * Configures connection to the Dictionary database.
 *
 * @author m314029
 * @since 1.2.0
 */
@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(
		entityManagerFactoryRef = "dictionaryEntityManagerFactory",
		transactionManagerRef = "dictionaryTransactionManager",
		basePackages = { "com.heb.pm.dictionary.repository" }
)
@Profile({"local", "dev", "cert", "prod"})
public class DictionaryDbConfig {

	@Value("${dictionary.datasource.schema}")
	private transient String dictionarySchema;

	/**
	 * Creates a bean for the connection to Dictionary.
	 *
	 * @return A bean for the connection to Dictionary.
	 */
	@Bean(name = "dictionaryDataSource")
	@ConfigurationProperties(prefix = "dictionary.datasource")
	public DataSource dictionaryDataSource() {
		return DataSourceBuilder.create().build();
	}

	/**
	 * Creates a bean for the entity manager for the Dictionary.
	 *
	 * @return A bean for the entity manager for the Dictionary.
	 */
	@Bean(name = "dictionaryEntityManagerFactory")
	public LocalContainerEntityManagerFactoryBean entityManagerFactory(
			EntityManagerFactoryBuilder builder,
			@Qualifier("dictionaryDataSource") DataSource dataSource) {

		HashMap<String, Object> properties = new HashMap<>();
		properties.put("hibernate.default_schema", dictionarySchema);

		return builder
				.dataSource(dataSource)
				.packages("com.heb.pm.dictionary.entity")
				.persistenceUnit("dictionaryEntityManagerFactory")
				.properties(properties)
				.build();
	}

	/**
	 * Creates a bean for the transaction manager for the Dictionary.
	 *
	 * @return A bean for the transaction manager for the Dictionary.
	 */
	@Bean(name = "dictionaryTransactionManager")
	public JpaTransactionManager transactionManager(
			@Qualifier("dictionaryEntityManagerFactory") EntityManagerFactory dictionaryEntityManagerFactory) {
		return new JpaTransactionManager(dictionaryEntityManagerFactory);
	}
}
