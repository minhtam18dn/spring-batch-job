package com.heb.pm.config;

import com.heb.pm.util.security.wsag.ClientInfoService;
import com.heb.pm.util.security.wsag.WsagAuthenticationProvider;
import com.heb.pm.util.security.wsag.WsagAuthenticationProviderBuilder;
import com.heb.pm.util.security.wsag.WsagTokenFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;

/**
 * Configures endpoint security for the application.
 *
 * @author d116773
 * @since 1.1.0
 */
@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
@Profile({"local", "dev", "cert", "prod"})
public class SecurityConfig extends WebSecurityConfigurerAdapter {


	@Autowired
	private transient Environment environment;

	/**
	 * Constructs a ClientInfoService bean.
	 *
	 * @return A ClientInfoService bean.
	 */
	@Bean
	public ClientInfoService clientInfoService() {
		return new ClientInfoService();
	}

	/**
	 * Returns a WsagAuthenticationProvider as a bean.
	 *
	 * @return A WsagAuthenticationProvider.
	 */
	@Bean
	public WsagAuthenticationProvider wsagAuthenticationProvider() {
		WsagAuthenticationProviderBuilder builder = new WsagAuthenticationProviderBuilder();
		return builder.allowAnonymousAccess().useProperties(this.environment).build();
	}

	/**
	 * Configures endpoint security.
	 *
	 * @param http The HttpSecurity object used to configure security.
	 * @throws Exception
	 */
	@Override
	public void configure(HttpSecurity http) throws Exception {
		http.csrf().disable()
				.authenticationProvider(wsagAuthenticationProvider())
				.sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS).and()
				.authorizeRequests()
				// This allows swagger to be accessed without an API key.
				.antMatchers("/v2/api-docs**",
					"/swagger.json",
					"/configuration/ui**",
					"/swagger-resources/**",
					"/configuration/security/**",
					"/swagger-ui.html",
					"/webjars/**").permitAll()
				// Everything else requires an API key.
				.anyRequest().fullyAuthenticated().and()
				.addFilterAfter(new WsagTokenFilter("pmApiKey"), BasicAuthenticationFilter.class)
				;
	}

	/**
	 * Configures the AuthenticationManager.
	 *
	 * @param auth The AuthenticationManagerBuilder used to configure the authentication manager.
	 * @throws Exception
	 */
	@Override
	public void configure(AuthenticationManagerBuilder auth) throws Exception {
		auth.authenticationProvider(wsagAuthenticationProvider());
	}
}
