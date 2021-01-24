package com.heb.pm.core.endpoint;

import com.heb.pm.core.endpoint.requests.ActivationRequest;
import com.heb.pm.core.exception.InvalidRequestException;
import com.heb.pm.core.service.activation.ActivationService;
import com.heb.pm.util.ValidatorUtils;
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
 * REST endpoint to kick off activations.
 *
 * @author d116773
 * @since 1.10.0
 */
@RestController
@RequestMapping("/activate")
@Api(value = "Activation Endpoint",
		consumes = MediaType.APPLICATION_JSON_VALUE,
		produces = MediaType.APPLICATION_JSON_VALUE)
public class ActivationEndpoint {

	private static final Logger logger = LoggerFactory.getLogger(ActivationEndpoint.class);

	@Autowired
	private transient ActivationService activationService;

	/**
	 * Activates a candidate or a collection of candidates.
	 *
	 * @param activationRequest The reqeust of what to activate. It should have either a work request ID or a tracking
	 *                          ID set.
	 * @param request The HTTP Servlet request that initiated this response.
	 * @return The text OK if the activation was successful.
	 */
	@PostMapping
	@ApiOperation("Activates a candidate.")
	@ApiResponses({
			@ApiResponse(code = 200, message = "The tracking ID associated with the activation.", response = Long.class),
			@ApiResponse(code = 400, message = "Activation request invalid."),
			@ApiResponse(code = 409, message = "Candidate activation failed.")
	})
	@PreAuthorize("hasAuthority('ACTIVATE_CANDIDATE')")
	public Long activate(@RequestBody ActivationRequest activationRequest, HttpServletRequest request) {

		validateRequest(activationRequest);

		logger.info(String.format("User %s from IP %s has issued the following activation request: %s.",
				activationRequest.getUserId(), request.getRemoteAddr(), activationRequest));

		// If they've provided a tracking ID, activate all the requests that are part of that.
		if (Objects.nonNull(activationRequest.getTrackingId())) {
			return this.activationService.activateByTrackingId(activationRequest.getTrackingId(), activationRequest.getUserId(),
					activationRequest.getRunAsynchronously(), Boolean.FALSE);
		}

		// If not, then activate by work request ID.
		return this.activationService.activateByWorkRequestId(activationRequest.getWorkRequestId(), activationRequest.getUserId(), Boolean.FALSE);
	}

	/**
	 * Activates a candidate or a collection of candidates variant.
	 *
	 * @param activationRequest The reqeust of what to activate. It should have either a work request ID or a tracking
	 *                          ID set.
	 * @param request The HTTP Servlet request that initiated this response.
	 * @return The text OK if the activation was successful.
	 */
	@PostMapping("/activeVariant")
	@ApiOperation("Activates a variant product.")
	@ApiResponses({
			@ApiResponse(code = 200, message = "The tracking ID associated with the activation.", response = Long.class),
			@ApiResponse(code = 400, message = "Activation request invalid."),
			@ApiResponse(code = 409, message = "Candidate activation failed.")
	})
	@PreAuthorize("hasAuthority('ACTIVATE_VARIANT')")
	public Long activateVariant(@RequestBody ActivationRequest activationRequest, HttpServletRequest request) {

		validateRequest(activationRequest);
		logger.info(String.format("User %s from IP %s has issued the following activation request: %s.",
				activationRequest.getUserId(), request.getRemoteAddr(), activationRequest));

		// If they've provided a tracking ID, activate all the requests that are part of that.
		if (Objects.nonNull(activationRequest.getTrackingId())) {
			return this.activationService.activateByTrackingId(activationRequest.getTrackingId(), activationRequest.getUserId(),
					activationRequest.getRunAsynchronously(), Boolean.TRUE);
		}

		// If not, then activate by work request ID.
		return this.activationService.activateByWorkRequestId(activationRequest.getWorkRequestId(), activationRequest.getUserId(), Boolean.TRUE);
	}

	/**
	 * Checks to see if an activation request is valid. Will throw an exception if it is not.
	 *
	 * @param activationRequest The activation request to check.
	 */
	private static void validateRequest(ActivationRequest activationRequest) {

		// Either tracking ID or work request ID must be specified, but you cant set both.
		if (ValidatorUtils.notExclusive(activationRequest.getTrackingId(), activationRequest.getWorkRequestId())) {

			throw new InvalidRequestException("Either tracking ID or work request ID must be provided, not both.");
		}

		// We need to know who kicked off activation.
		if (Objects.isNull(activationRequest.getUserId())) {
			throw new InvalidRequestException("User ID is required.");
		}

		// If they didn't provide a value for runAsynchronously, the default is false.
		if (Objects.isNull(activationRequest.getRunAsynchronously())) {
			activationRequest.setRunAsynchronously(Boolean.FALSE);
		}
	}

}
