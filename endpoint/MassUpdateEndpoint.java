package com.heb.pm.core.endpoint;

import com.heb.pm.core.exception.ValidationException;
import com.heb.pm.core.maintenance.MassUpdateRequest;
import com.heb.pm.core.service.massupdate.MassUpdateService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.Objects;

/**
 * REST endpoint to handle mass udpate requests.
 *
 * @author d116773
 * @since 1.22.0
 */
@RestController
@RequestMapping("/massupdate")
@Api(value = "Mass Update Endpoint", produces = MediaType.TEXT_PLAIN_VALUE)
public class MassUpdateEndpoint {

	private static final Logger logger = LoggerFactory.getLogger(MassUpdateEndpoint.class);

	@Autowired
	private transient MassUpdateService massUpdateService;

	/**
	 * Kicks off a mass update process.
	 *
	 * @param massUpdateRequest The mass update request.
	 * @return The tracking ID from the request.
	 */
	@PostMapping
	@PreAuthorize("hasAuthority('MASS_UPDATE')")
	@ApiOperation("Kicks off a mass update job.")
	@ApiResponses({
			@ApiResponse(code = 200, message = "The original mass update request.", response = MassUpdateRequest.class),
			@ApiResponse(code = 409, message = "Unable to validate request."),
			@ApiResponse(code = 500, message = "Unable to run mass update.")
	})
	public MassUpdateRequest massUpdate(@RequestBody MassUpdateRequest massUpdateRequest, HttpServletRequest request) {

		if (Objects.isNull(massUpdateRequest.getUserId())) {
			throw new ValidationException("Unable to validate mass update request.", "User ID is required");
		}

		logger.info(String.format("User %s from IP %s has issued the following mass update request: %s.",
				massUpdateRequest.getUserId(), request.getRemoteAddr(), massUpdateRequest));

		this.massUpdateService.executeMassUpdateRequest(massUpdateRequest);
		return massUpdateRequest;
	}
}
