package com.heb.pm.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.core.support.BaseLdapPathContextSource;
import org.springframework.security.ldap.DefaultSpringSecurityContextSource;

/**
 * Configures connection to LDAP.
 *
 * @author m314029
 * @since 1.2.0
 */
@Configuration
@Profile({"local", "dev", "cert", "prod"})
public class LDAPDbConfig {

	// ldap values
	@Value("${heb.ldap.url}")
	private transient String url;
	@Value("${heb.ldap.root}")
	private transient String root;
	@Value("${heb.ldap.managerDn}")
	private transient String managerDn;
	@Value("${heb.ldap.managerPassword}")
	private transient String managerPassword;

	/**
	 * Returns the LdapTemplate connected to HEB's internal LDAP provider. It is created as a Spring
	 * managed bean with the ID hebLdapTemplate.
	 *
	 * @return The LdapTemplate connected to HEB's internal LDAP provider.
	 * @throws Exception Any error during the construction of the LdapTemplate.
	 */
	@Bean(name = "hebLdapTemplate")
	public LdapTemplate ldapTemplate() throws Exception {
		LdapTemplate ldapTemplate = new LdapTemplate();
		ldapTemplate.setContextSource(contextSource());
		ldapTemplate.afterPropertiesSet();

		return ldapTemplate;
	}

	/**
	 * Returns the LDAP context for HEB's internal LDAP provider. It is created as a Spring managed
	 * bean witht he ID hebLdapContext.
	 *
	 * @return The LDAP context for HEB's internal LDAP provider.
	 */
	@Bean(name = "hebLdapContext")
	public BaseLdapPathContextSource contextSource() {
		DefaultSpringSecurityContextSource contextSource =
				new DefaultSpringSecurityContextSource(this.url);
		contextSource.setUserDn(this.managerDn);
		contextSource.setPassword(this.managerPassword);
		contextSource.setBase(this.root);
		contextSource.afterPropertiesSet();

		return contextSource;
	}
}
