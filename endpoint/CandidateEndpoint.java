package com.heb.pm.core.endpoint;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.heb.pm.core.service.candidate.CandidateService;
import com.heb.pm.core.service.candidate.CandidateValidator;
import com.heb.pm.core.service.validators.CandidateValidatorType;
import com.heb.pm.pam.model.Candidate;
import com.heb.pm.pam.model.CandidateWrapper;
import io.swagger.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.StringJoiner;

// TODO: move all the candidate code away from the core package (maybe).
/**
 * Candidate REST endpoint.
 *
 * @author a786878
 * @since 1.0.0
 */
@RestController
@RequestMapping("/candidates")
@Api(value = "CandidateEndpointAPI",
		consumes = MediaType.APPLICATION_JSON_VALUE,
		produces = MediaType.APPLICATION_JSON_VALUE)
public class CandidateEndpoint {

	private static final String ACTIVATE_CANDIDATE_LOG_MESSAGE =
			"A user %s from IP %s requested to activate a candidate and send the response to %s.";
	private static final String VALIDATE_LOG_MESSAGE = "User %s requested validation using validators %s for candidate %d.";

	private static final Logger logger = LoggerFactory.getLogger(CandidateEndpoint.class);

	@Autowired
	private transient CandidateService candidateService;

	@Autowired
	private transient ObjectMapper objectMapper;

	@Autowired
	private transient CandidateValidator candidateValidator;

	/**
	 * Activates a candidate.
	 *
	 * @param candidate The candidate to activate.
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/activateCandidate")
	@ApiOperation("Activates a candidate.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "Candidate was activated."),
			@ApiResponse(code = 500, message = "Error activating candidate."),
			@ApiResponse(code = 409, message = "Candidate failed validation.")
	})
	@PreAuthorize("hasAuthority('ACTIVATE_CANDIDATE')")
	public String activateCandidate(@ApiParam("The user ID of the requestor.")
									@RequestParam("user") String user,

									@ApiParam("The responseURL to send the response to.")
									@RequestParam("responseURL") String responseURL,

									@RequestBody Candidate candidate, HttpServletRequest request) {

		CandidateEndpoint.logger.info(
				String.format(ACTIVATE_CANDIDATE_LOG_MESSAGE, user, request.getRemoteAddr(), responseURL));

		if (logger.isDebugEnabled()) {
			try {
				logger.debug(this.objectMapper.writeValueAsString(candidate));
			} catch (JsonProcessingException e) {
				logger.debug(e.getLocalizedMessage());
			}
		}

		CandidateService.ActivationRequest activationRequest = new CandidateService.ActivationRequest(user, responseURL, candidate);

		return this.candidateService.activateCandidate(activationRequest);
	}

	/**
	 * Validates a candidate using the named validators.
	 *
	 * @param user The ID of the user triggering the validation.
	 * @param validatorTypes The types of validation to run.
	 * @param candidateWrapper contains The candidate to validate.
	 * @return The original candidate sent to the request.
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/validate")
	@ApiOperation("Validates a candidate has all the information for the named validators.")
	@PreAuthorize("hasAuthority('ACTIVATE_CANDIDATE')")
	public Candidate validate(
			@ApiParam("The user ID of the requestor.")
			@RequestParam("user") String user,

			@ApiParam("The named validators. Valid values are: VENDOR_DATA_VALIDATOR, BUYER_DATA_VALIDATOR," +
					" SCA_DATA_VALIDATOR, UPC_VALIDATOR, CASE_UPC_VALIDATOR, SUPPLIER_NEW_PRODUCT_SETUP_VALIDATOR," +
					" SUPPLIER_HEB_SETUP_VALIDATOR, SUPPLIER_PRODUCT_DETAILS_VALIDATOR, SUPPLIER_CASE_PACK_VALIDATOR, COST_LINK_VALIDATOR")


			@RequestParam("validator") List<CandidateValidatorType> validatorTypes,

			@RequestBody CandidateWrapper candidateWrapper) {

		StringJoiner listOfValidators = new StringJoiner(",");
		validatorTypes.forEach(v -> listOfValidators.add(v.name()));
		CandidateEndpoint.logger.info(String.format(CandidateEndpoint.VALIDATE_LOG_MESSAGE, user,
				listOfValidators.toString(), candidateWrapper.getCandidate().getCandidateId()));

		this.candidateValidator.validate(validatorTypes, candidateWrapper);
		return candidateWrapper.getCandidate();
	}
}
