package com.heb.pm.core.endpoint;

import com.heb.pm.core.exception.NotFoundException;
import com.heb.pm.core.model.CostLink;
import com.heb.pm.core.service.CostLinkService;
import com.heb.pm.pam.model.CostLinkByItemCodeRequest;
import com.heb.pm.pam.model.CostLinkRequest;
import io.swagger.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

/**
 * Rest endpoint for finding cost link information.
 */
@RestController()
@RequestMapping(CostLinkEndpoint.COST_LINK_BASE_URL)
@Api(value = "CostLinkAPI", produces = MediaType.APPLICATION_JSON_VALUE)
public class CostLinkEndpoint {

	private static final Logger logger = LoggerFactory.getLogger(CostLinkEndpoint.class);

	protected static final String COST_LINK_BASE_URL = "/costLink";

	private static final String BY_COST_LINK_URL = "{costLink}";
	private static final String BY_ITEM_CODE_URL = "/item/{itemCode}";
	private static final String BY_COST_LINK_AND_AP_NUMBER_LINK_URL = "{costLink}/{apNumber}";
	private static final String BY_ITEM_CODE_AND_AP_NUMBER_URL = "/item/{itemCode}/{apNumber}";

	@Autowired
	private transient CostLinkService costLinkService;

	/**
	 *  Returns cost link information by cost link ID. This will set the HTTP status to 404 if not found.
	 *
	 * @param costLinkNumber The cost link being requested.
	 * @param request The HTTP servlet request that initiated the request.
	 * @return The item information for the requested item.
	 */
	@RequestMapping(method = RequestMethod.GET, value = BY_COST_LINK_URL)
	@ApiOperation("Returns cost link information for a given cost link.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "costLink\" : {costLink}", response = CostLink.class),
			@ApiResponse(code = 404, message = "error : Cost link not found.")})
	public CostLink getCostLink(@ApiParam("The ID of the cost link being requested.")
								@PathVariable("costLink") Long costLinkNumber,
								HttpServletRequest request) {

		CostLinkEndpoint.logger.info(String.format("A user from IP %s requested information for cost link number %d.",
				request.getRemoteAddr(), costLinkNumber));

		return this.costLinkService.getCostLinkById(new CostLinkRequest().setCostLinkNumber(costLinkNumber))
				.orElseThrow(NotFoundException.NOT_FOUND_EXCEPTION_SUPPLIER);
	}

	/**
	 *  Returns cost link information for a given item code. This will set the HTTP status to 404 if not found.
	 *
	 * @param itemCode The item code being requested.
	 * @param request The HTTP servlet request that initiated the request.
	 * @return The item information for the requested item.
	 */
	@RequestMapping(method = RequestMethod.GET, value = BY_ITEM_CODE_URL)
	@ApiOperation("Returns cost link information for a given item code.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "costLink\" : {costLink}", response = CostLink.class),
			@ApiResponse(code = 404, message = "error : Cost link not found.")})
	public CostLink getCostLinkByItem(@ApiParam("The item code to find cost link information for.")
									  @PathVariable("itemCode") Long itemCode,
									  HttpServletRequest request) {

		CostLinkEndpoint.logger.info(String.format("A user from IP %s requested cost link information for item code %d.",
				request.getRemoteAddr(), itemCode));

		return this.costLinkService.getCostLinkByItemCode(new CostLinkByItemCodeRequest().setItemCode(itemCode))
				.orElseThrow(NotFoundException.NOT_FOUND_EXCEPTION_SUPPLIER);
	}
	/**
	 *  Returns cost link information by cost link ID. This will set the HTTP status to 404 if not found.
	 *
	 * @param costLinkNumber The cost link being requested.
	 * @param apNumber The ap number of the cost link being requested.
	 * @param request The HTTP servlet request that initiated the request.
	 * @return The item information for the requested item.
	 */
	@RequestMapping(method = RequestMethod.GET, value = BY_COST_LINK_AND_AP_NUMBER_LINK_URL)
	@ApiOperation("Returns cost link information for a given cost link and ap number.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "costLink\" : {costLink}", response = CostLink.class),
			@ApiResponse(code = 404, message = "error : Cost link not found.")})
	public CostLink getCostLink(@ApiParam("The ID of the cost link being requested.")
								@PathVariable("costLink") Long costLinkNumber,
								@ApiParam("The AP Number to search against.")
								@PathVariable("apNumber") Long apNumber,
								HttpServletRequest request) {

		CostLinkEndpoint.logger.info(String.format("A user from IP %s requested information for cost link number %d.",
				request.getRemoteAddr(), costLinkNumber));

		return this.costLinkService.getCostLinkById(new CostLinkRequest()
				.setCostLinkNumber(costLinkNumber)
				.setApNumber(apNumber))
				.orElseThrow(NotFoundException.NOT_FOUND_EXCEPTION_SUPPLIER);
	}

	/**
	 *  Returns cost link information for a given item code. This will set the HTTP status to 404 if not found.
	 *
	 * @param itemCode The item code being requested.
	 * @param apNumber The ap number of the cost link being requested.
	 * @param request The HTTP servlet request that initiated the request.
	 * @return The item information for the requested item.
	 */
	@RequestMapping(method = RequestMethod.GET, value = BY_ITEM_CODE_AND_AP_NUMBER_URL)
	@ApiOperation("Returns cost link information for a given item code.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "costLink\" : {costLink}", response = CostLink.class),
			@ApiResponse(code = 404, message = "error : Cost link not found.")})
	public CostLink getCostLinkByItem(@ApiParam("The item code to find cost link information for.")
									  @PathVariable("itemCode") Long itemCode,
									  @ApiParam("The AP Number to search against.")
									  @PathVariable("apNumber") Long apNumber,
									  HttpServletRequest request) {

		CostLinkEndpoint.logger.info(String.format("A user from IP %s requested cost link information for item code %d.",
				request.getRemoteAddr(), itemCode));

		return this.costLinkService.getCostLinkByItemCode(new CostLinkByItemCodeRequest()
				.setItemCode(itemCode)
				.setApNumber(apNumber))
				.orElseThrow(NotFoundException.NOT_FOUND_EXCEPTION_SUPPLIER);
	}
}
