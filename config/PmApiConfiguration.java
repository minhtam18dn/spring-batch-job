/*
 *  PmApiConfiguration
 *
 *  Copyright (c) 2018 HEB
 *  All rights reserved.
 *
 *  This software is the confidential and proprietary information of HEB.
 */

package com.heb.pm.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

/**
 * Configuration you want regardless of environment.
 *
 * @author d116773
 * @since 1.0.0
 */
@Configuration
public class PmApiConfiguration {

	/**
	 * Constructs a Jackson Object Mapper to use when serializing objects to JSON.
	 *
	 * @return The Jackson Object Mapper to use when serializing objects to JSON.
	 */
	@Primary
	@Bean
	public ObjectMapper getJacksonBuilder() {

		Jackson2ObjectMapperBuilder objectMapperBuilder = new Jackson2ObjectMapperBuilder();

		// The first will cause instants to be written as nanoseconds so that they can be instantiated as dates in JavaScript.
		// The second will allow the dates coming back from JavaScript to be converted to instants.
		// The other two are turned off by Spring by default, so this is mapping that.
		// Note that turning off DEFAULT_VIEW_INCLUSION means that if you annotate a controller as
		// having a JSON view, then anything not annotated will not be included.
		objectMapperBuilder.featuresToDisable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS,
				DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS,
				MapperFeature.DEFAULT_VIEW_INCLUSION,
				DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);


		// This will cause collections to be serialized as an array even if there is only one value
		// in the collection.
		objectMapperBuilder.featuresToEnable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);

		// This will cause null values to not be serialized by default.
		objectMapperBuilder.serializationInclusion(JsonInclude.Include.NON_NULL);

		// This will allow for jackson optionals.
		objectMapperBuilder.modulesToInstall(new Jdk8Module());

		return objectMapperBuilder.build();
	}


}
