package com.heb.pm.batch;

import org.springframework.batch.core.ExitStatus;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Exception to throw when a job fails.
 *
 * @author d116773
 * @since 1.10.0
 */
@ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
public class JobFailedException extends RuntimeException {

	private static final long serialVersionUID = 1701905470708659100L;

	/**
	 * Constructs a new ActivationFailedException.
	 *
	 * @param exitStatus The exit status of the activation routine.
	 */
	/* default */ JobFailedException(ExitStatus exitStatus) {

		super (String.format(exitStatus.getExitDescription().toString()));
	}
}
