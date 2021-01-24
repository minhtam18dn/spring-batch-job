package com.heb.pm.core.endpoint;

import com.heb.pm.core.exception.NotFoundException;
import com.heb.pm.core.exception.ValidationException;
import com.heb.pm.core.maintenance.BaseUpcMaintenanceRequest;
import com.heb.pm.core.model.DsdItem;
import com.heb.pm.core.service.DsdItemService;
import com.heb.pm.core.service.maintenance.DsdUpcMaintenanceService;
import com.heb.pm.core.service.maintenance.swap.ExtendedBothToDsdRequest;
import com.heb.pm.core.service.maintenance.swap.ExtendedDsdToBothRequest;
import com.heb.pm.util.security.wsag.ClientInfoService;
import io.swagger.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Objects;

/**
 * REST endpoint for DSD item information.
 *
 * @author d116773
 * @since 1.12.0
 */
@RestController()
@RequestMapping("/dsd")
@Api(value = "DSD API", produces = MediaType.APPLICATION_JSON_VALUE)
public class DsdEndpoint {

	private static final Logger logger = LoggerFactory.getLogger(DsdEndpoint.class);

	@Autowired
	private transient DsdItemService dsdItemService;

	@Autowired
	private transient DsdUpcMaintenanceService dsdUpcMaintenanceService;

	@Autowired
	private transient ClientInfoService clientInfoService;

	/**
	 *  Returns DSD item information for a given UPC.
	 *
	 * @param dsdUpc The UPC being requested.
	 * @param request The HTTP servlet request that initiated the request.
	 * @return The item information for the requested DSD item.
	 */
	@RequestMapping(method = RequestMethod.GET, value = "/{dsdUpc}")
	@ApiOperation("Returns DSD item information linked to the given UPC.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "dsdUpc\" : {dsdUpc}", response = DsdItem.class),
			@ApiResponse(code = 404, message = "error : UPC not found.")})
	public DsdItem getDsdItemInfo(
			@ApiParam("The DSD UPC being requested.")
			@PathVariable("dsdUpc") Long dsdUpc,
			HttpServletRequest request) {

		DsdEndpoint.logger.info(String.format("Application %s from IP %s requesed DSD UPC information for %d.",
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), dsdUpc));

		return this.dsdItemService.getDsdItem(dsdUpc).orElseThrow(NotFoundException.NOT_FOUND_EXCEPTION_SUPPLIER);
	}

	/**
	 * Takes a DSD only item and adds it to an item code, making it a both item.
	 *
	 * @param dsdUpc The DSD item to convert to a both item.
	 * @param dsdToBothRequest The request containing the item and additional info.
	 * @param request The HTTP servlet request that initiated this function.
	 * @return The modified DSD item.
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/{dsdUpc}/toBoth")
	@ApiOperation("Takes in a DSD UPC and attaches it to a warehouse item code making it a both item.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "The updated item.", response = DsdItem.class),
			@ApiResponse(code = 409, message = "Invalid request."),
			@ApiResponse(code = 500, message = "DSD to Both operation failed.")})
	@PreAuthorize("hasAuthority('SWAP')")
	public DsdItem dsdToBoth(
			@ApiParam("The DSD UPC to make a both item.")
			@PathVariable("dsdUpc") Long dsdUpc,
			@ApiParam("The DSD to Both request.") @RequestBody ExtendedDsdToBothRequest dsdToBothRequest,
						  HttpServletRequest request) {

		if (Objects.nonNull(dsdToBothRequest.getRunId())) {
			throw new ValidationException("Unable to perform DSD to both operation.", List.of("This endpoint cannot be used to reprocess swap requests."));
		}

		dsdToBothRequest.setScanCodeId(dsdUpc);

		return this.dsdToBoth(dsdToBothRequest, request);
	}

	/**
	 * Takes a DSD only item and adds it to an item code, making it a both item. This function is meant for production
	 * support to be able to replay requests that failed previously.
	 *
	 * @param dsdUpc The DSD item to convert to a both item.
	 * @param dsdToBothRequest The request containing the item and additional info.
	 * @param request The HTTP servlet request that initiated this function.
	 * @return The modified DSD item.
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/{dsdUpc}/support/toBoth")
	@PreAuthorize("hasAuthority('REPLAY_SWAP')")
	@ApiOperation(value = "DSD to Both for production support.", hidden = true)
	public DsdItem dsdToBothAsSupport(
			@PathVariable("dsdUpc") Long dsdUpc,
			@RequestBody ExtendedDsdToBothRequest dsdToBothRequest,
			HttpServletRequest request) {

		this.validateReprocess(dsdToBothRequest, dsdUpc, dsdToBothRequest.getScanCodeId());

		return this.dsdToBoth(dsdToBothRequest, request);
	}

	/**
	 * Delegate for kicking off a DSD to Both operation.
	 *
	 * @param dsdToBothRequest The request containing the item and additional info.
	 * @param request The HTTP servlet request that initiated this function.
	 * @return The modified DSD item.
	 */
	private DsdItem dsdToBoth(ExtendedDsdToBothRequest dsdToBothRequest, HttpServletRequest request) {

		if (Objects.isNull(dsdToBothRequest.getUserId())) {
			throw new ValidationException("Unable to perform DSD to both operation.", List.of("User ID is required."));
		}

		DsdEndpoint.logger.info(String.format("User %s using application %s from IP %s requested to perform a DSD to both operation: '%s'.",
				dsdToBothRequest.getUserId(), this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), dsdToBothRequest.toString()));

		this.dsdUpcMaintenanceService.dsdToBoth(dsdToBothRequest);

		return this.dsdItemService.getDsdItem(dsdToBothRequest.getScanCodeId())
				.orElseThrow(NotFoundException.NOT_FOUND_EXCEPTION_SUPPLIER);
	}

	/**
	 * Takes a both item and removes the DSD item from the warehouse item.
	 *
	 * @param dsdUpc The DSD item to remove from the warehouse item.
	 * @param bothToDsdRequest The request containing the item and additional info.
	 * @param request The HTTP servlet request that initiated this function.
	 * @return The modified DSD item.
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/{dsdUpc}/fromBoth")
	@ApiOperation("Takes in a both item and separates out the DSD component.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "The updated item.", response = DsdItem.class),
			@ApiResponse(code = 409, message = "Invalid request."),
			@ApiResponse(code = 500, message = "Both to DSD operation failed.")})
	@PreAuthorize("hasAuthority('SWAP')")
	public DsdItem bothToDsd(
			@ApiParam("The DSD UPC to make a both item.")
			@PathVariable("dsdUpc") Long dsdUpc,
			@ApiParam("The Both to DSD request.")
			@RequestBody ExtendedBothToDsdRequest bothToDsdRequest,
						  HttpServletRequest request) {

		if (Objects.nonNull(bothToDsdRequest.getRunId())) {
			throw new ValidationException("Unable to perform both to DSD operation.", List.of("This endpoint cannot be used to reprocess swap requests."));
		}

		bothToDsdRequest.setScanCodeId(dsdUpc);

		return this.bothToDsd(bothToDsdRequest, request);
	}


	/**
	 * Takes a both item and removes the DSD item from the warehouse item. This function is meant for production
	 * support to be able to replay requests that failed previously.
	 *
	 * @param dsdUpc The DSD item to remove from the warehouse item.
	 * @param bothToDsdRequest The request containing the item and additional info.
	 * @param request The HTTP servlet request that initiated this function.
	 * @return The modified DSD item.
	 */
	@RequestMapping(method = RequestMethod.POST, value = "/{dsdUpc}/support/fromBoth")
	@PreAuthorize("hasAuthority('REPLAY_SWAP')")
	@ApiOperation(value = "Both to DSD for production support.", hidden = true)
	public DsdItem bothToDsdAsSupport(
			@PathVariable Long dsdUpc,
			@RequestBody ExtendedBothToDsdRequest bothToDsdRequest,
			HttpServletRequest request) {

		this.validateReprocess(bothToDsdRequest, dsdUpc, bothToDsdRequest.getScanCodeId());

		return this.bothToDsd(bothToDsdRequest, request);
	}

	/**
	 * Delegate for kicking off a both to DSD request.
	 *
	 * @param bothToDsdRequest The both to DSD request from the user.
	 * @param request servlet request that initiated this function.
	 * @return The modified DSD item.
	 */
	private DsdItem bothToDsd(ExtendedBothToDsdRequest bothToDsdRequest, HttpServletRequest request) {

		if (Objects.isNull(bothToDsdRequest.getUserId())) {
			throw new ValidationException("Unable to perform both to DSD operation.", List.of("User ID is required."));
		}

		DsdEndpoint.logger.info(String.format("User %s using application %s from IP %s requested to perform a both to DSD operation: '%s'.",
				bothToDsdRequest.getUserId(), this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), bothToDsdRequest.toString()));

		this.dsdUpcMaintenanceService.bothToDsd(bothToDsdRequest);

		return this.dsdItemService.getDsdItem(bothToDsdRequest.getScanCodeId())
				.orElseThrow(NotFoundException.NOT_FOUND_EXCEPTION_SUPPLIER);
	}

	private void validateReprocess(BaseUpcMaintenanceRequest upcMaintenanceRequest, Long pathUpc, Long requestUpc) {

		if (Objects.isNull(upcMaintenanceRequest.getRunId())) {
			throw new ValidationException("Unable to perform operation.", List.of("This endpoint requires an already processed swap requests."));
		}

		if (!Objects.equals(pathUpc, requestUpc)) {
			throw new ValidationException("Unable to perform operation.", List.of("The path UPC and the request UPC must match."));
		}
	}
}
