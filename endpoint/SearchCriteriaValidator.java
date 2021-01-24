package com.heb.pm.core.endpoint;

import com.heb.pm.core.model.ProductSearchCriteria;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.util.Objects;

/**
 * Provides functions to make sure the search criteria has a valid set of parameters.
 *
 * @author d116773
 * @since 1.4.0
 */
/* default */ final class SearchCriteriaValidator {


	/**
	 * Exception to throw when the user's search criteria does not pass validation.
	 *
	 * @author d116773
	 * @since 1.4.0
	 */
	@ResponseStatus(HttpStatus.BAD_REQUEST)
	/* default */ static final class ValidationException extends RuntimeException {

		private static final long serialVersionUID = -4512252836632545440L;

		/* default */ ValidationException(String message) {

			super(message);
		}
	}

	// Private constructor.
	private SearchCriteriaValidator() {
	}

	/**
	 * Inspects search criteria to make sure the search request is valid. The method will throw an exception
	 * if the criteria to not validate.
	 *
	 * @param searchCriteria The SearchCriteria to inspect.
	 */
	public static void validate(ProductSearchCriteria searchCriteria) {

		// Currently, a list of product IDs is required, so make sure it's there.
		if (Objects.isNull(searchCriteria.getProductIds()) || searchCriteria.getProductIds().isEmpty()) {
			throw new ValidationException("No search parameters defined.");
		}
	}
}
