package com.heb.pm.arbaf;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Exception to throw when an application is not found in ARBAF.
 *
 * @author d116773
 * @since 1.1.0
 */
@ResponseStatus(HttpStatus.NOT_FOUND)
public class ApplicationNotFoundException extends RuntimeException {

	private static final long serialVersionUID = -466880882466582602L;

	/**
	 * Constructs a new ApplicationNotFoundException.
	 *
	 * @param applicationName The application that is not found.
	 */
	/* default */ ApplicationNotFoundException(String applicationName) {
		super(String.format("Application \"%s\" not found.", applicationName));
	}
}
