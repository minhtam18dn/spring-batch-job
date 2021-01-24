package com.heb.pm.core.endpoint;

import com.heb.pm.core.endpoint.requests.EventStagingRequest;
import com.heb.pm.core.endpoint.requests.ItemEventRequest;
import com.heb.pm.core.endpoint.requests.ProductEventRequest;
import com.heb.pm.core.endpoint.requests.UpcEventRequest;
import com.heb.pm.core.exception.InvalidRequestException;
import com.heb.pm.core.service.ProductionSupportService;
import com.heb.pm.dao.core.entity.codes.ItemKeyType;
import com.heb.pm.util.security.wsag.ClientInfoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.Objects;

/**
 * REST endpoint for production support related functions.
 *
 * @author d116773
 * @since 1.8.0
 */
@RestController()
@RequestMapping(ProductionSupportEndpoint.PRODUCTION_SUPPORT_BASE_URL)
public class ProductionSupportEndpoint {

	private static final Logger logger = LoggerFactory.getLogger(ProductionSupportEndpoint.class);
	private static final int MAX_EVENT_STAGE_SIZE = 5_000;

	protected static final String PRODUCTION_SUPPORT_BASE_URL = "/support";

	protected static final String GENERATE_PRM2 = "/prm2";
	protected static final String GENERATE_PUPD_FOR_UPC = "/pupd/upc";
	protected static final String GENERATE_PUPD_FOR_DSD = "/pupd/dsd";
	protected static final String GENERATE_PUPD_FOR_WAREHOUSE = "/pupd/item";
	protected static final String NUTRITIONAL_CLAIMS = "/claims";

	@Autowired
	private transient ProductionSupportService productionSupportService;

	@Autowired
	private transient ClientInfoService clientInfoService;

	/**
	 * Stages PRM2 events.
	 *
	 * @param eventRequest The request with the user ID and list of products to stage events for.
	 * @param request The HttpServletRequest that triggered this call.
	 * @return OK if successful.
	 */
	@PreAuthorize("hasAuthority('STAGE_EVENT')")
	@RequestMapping(method = RequestMethod.POST, value = GENERATE_PRM2, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.TEXT_PLAIN_VALUE)
	public String stagePRM2(@RequestBody ProductEventRequest eventRequest, HttpServletRequest request) {

		validateEventRequest(eventRequest);
		logger.info(String.format("User %s from IP %s using application %s requested PRM2 events for %d products.", eventRequest.getUserId(),
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), eventRequest.getProductIds().size()));

		this.productionSupportService.stagePRM2(eventRequest.getUserId(), eventRequest.getProductIds());
		return "OK";
	}

	/**
	 * Stages PUPD events for UPCs.
	 *
	 * @param eventRequest The request with the user ID and list of UPCs to stage events for.
	 * @param request The HttpServletRequest that triggered this call.
	 * @return OK if successful.
	 */
	@PreAuthorize("hasAuthority('STAGE_EVENT')")
	@RequestMapping(method = RequestMethod.POST, value = GENERATE_PUPD_FOR_UPC, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.TEXT_PLAIN_VALUE)
	public String stagePUPDForUpcs(@RequestBody UpcEventRequest eventRequest, HttpServletRequest request) {

		validateEventRequest(eventRequest);
		logger.info(String.format("User %s from IP %s using application %s requested PUPD events for %d UPCs.", eventRequest.getUserId(),
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), eventRequest.getUpcs().size()));

		this.productionSupportService.stagePUPDForUpcs(eventRequest.getUserId(), eventRequest.getUpcs());
		return "OK";
	}

	/**
	 * Stages PUPD events for DSD Items.
	 *
	 * @param eventRequest The request with the user ID and list of DSD UPCs to stage events for.
	 * @param request The HttpServletRequest that triggered this call.
	 * @return OK if successful.
	 */
	@PreAuthorize("hasAuthority('STAGE_EVENT')")
	@RequestMapping(method = RequestMethod.POST, value = GENERATE_PUPD_FOR_DSD, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.TEXT_PLAIN_VALUE)
	public String stagePUPDForDsdItems(@RequestBody UpcEventRequest eventRequest, HttpServletRequest request) {

		validateEventRequest(eventRequest);
		logger.info(String.format("User %s from IP %s using application %s requested PUPD events for %d DSD items.", eventRequest.getUserId(),
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), eventRequest.getUpcs().size()));

		this.productionSupportService.stagePUPDForItems(eventRequest.getUserId(), eventRequest.getUpcs(), ItemKeyType.DSD);
		return "OK";
	}

	/**
	 * Stages PUPD events for Warehouse Items.
	 *
	 * @param eventRequest The request with the user ID and list of DSD UPCs to stage events for.
	 * @param request The HttpServletRequest that triggered this call.
	 * @return OK if successful.
	 */
	@PreAuthorize("hasAuthority('STAGE_EVENT')")
	@RequestMapping(method = RequestMethod.POST, value = GENERATE_PUPD_FOR_WAREHOUSE, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.TEXT_PLAIN_VALUE)
	public String stagePUPDForWarehouseItems(@RequestBody ItemEventRequest eventRequest, HttpServletRequest request) {

		validateEventRequest(eventRequest);
		logger.info(String.format("User %s from IP %s using application %s requested PUPD events for %d warehouse items.", eventRequest.getUserId(),
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), eventRequest.getItemCodes().size()));

		this.productionSupportService.stagePUPDForItems(eventRequest.getUserId(), eventRequest.getItemCodes(), ItemKeyType.WAREHOUSE);
		return "OK";
	}

	/**
	 * Stages NutritonalClaims events for a list of UPCs..
	 *
	 * @param eventRequest The request with the user ID and list of UPCs to stage events for.
	 * @param request The HttpServletRequest that triggered this call.
	 * @return OK if successful.
	 */
	@PreAuthorize("hasAuthority('STAGE_EVENT')")
	@RequestMapping(method = RequestMethod.POST, value = NUTRITIONAL_CLAIMS, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.TEXT_PLAIN_VALUE)
	public String stageNutritioanlClaims(@RequestBody UpcEventRequest eventRequest, HttpServletRequest request) {

		validateEventRequest(eventRequest);
		logger.info(String.format("User %s from IP %s using application %s requested nutritional claims events for %d UPCs.", eventRequest.getUserId(),
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), eventRequest.getUpcs().size()));

		this.productionSupportService.stageNutritionalClaimsEvents(eventRequest.getUpcs());
		return "OK";
	}


	/**
	 * Validates an EventStagingRequest.. This will throw an exception if it does not pass validation.
	 *
	 * @param eventStagingRequest The request to validate.
	 */
	private static void validateEventRequest(EventStagingRequest eventStagingRequest) {

		if (Objects.isNull(eventStagingRequest)) {
			throw new InvalidRequestException("Event request cannot be empty.");
		} else if (Objects.isNull(eventStagingRequest.getUserId())) {
			throw new InvalidRequestException("User ID is required when requesting event generation.");
		} else if (Objects.isNull(eventStagingRequest.getIdsToStage()) || eventStagingRequest.getIdsToStage().isEmpty()) {
			throw new InvalidRequestException("IDs are required when requesting event generation.");
		} else if (eventStagingRequest.getIdsToStage().size() > MAX_EVENT_STAGE_SIZE) {
			throw new InvalidRequestException(String.format("No more than %,d things can have events generated at a time.", MAX_EVENT_STAGE_SIZE));
		}
	}
}
