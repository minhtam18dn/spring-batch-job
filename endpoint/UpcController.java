/*
 * UpcController.java
 *
 * Copyright (c) 2018 HEB
 * All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of HEB.
 */
package com.heb.pm.core.endpoint;


import com.heb.pm.core.exception.InvalidRequestException;
import com.heb.pm.core.exception.NotFoundException;
import com.heb.pm.core.maintenance.NewUpcRequest;
import com.heb.pm.core.maintenance.UpcMaintenanceRequest;
import com.heb.pm.core.model.Upc;
import com.heb.pm.core.service.UpcService;
import com.heb.pm.core.service.maintenance.UpcMaintenanceService;
import com.heb.pm.core.service.nutrition.GenesisApprovalService;
import com.heb.pm.core.service.search.ExtendedSellingUnitSearchCriteria;
import com.heb.pm.util.ListUtils;
import com.heb.pm.util.endpoint.PageableResult;
import com.heb.pm.util.security.wsag.ClientInfoService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * REST endpoint for UPCs.
 *
 * @author s769046
 * @since 1.0.0
 */
@RestController()
@RequestMapping(UpcController.UPC_BASE_URL)
public class UpcController {

    private static final Logger logger = LoggerFactory.getLogger(UpcController.class);

    private static final String FIND_UPC =
            "Application %s from IP %s has requested information for by UPC: %d";

	private static final String NEW_UPC_MESSAGE =
			"User %s has requested a new UPC of type: %s";

    protected static final String UPC_BASE_URL = "/upc";

	private static final String NEW_UPC = "/new";

	private static final String BY_SEARCH_CRITERIA = "/search";

    private static final String BY_SEARCH_CRITERIA_STREAMING = "/search/stream";

    private static final String APPROVAL_NUTRITION = "/approval-nutrition";

    @Autowired
    private transient UpcService upcService;

    @Autowired
    private transient ClientInfoService clientInfoService;

    @Autowired
	private transient UpcMaintenanceService upcMaintenanceService;

    @Autowired
    private transient GenesisApprovalService genesisApprovalService;


    /**
     * Looks up information for a UPC. If the UPC is not found, will set the return status to 404.
     *
     * @param upc The UPC to look up information for.
     * @param request The HTTP servlet request that initiated the request.
     * @return The UPC requested.
     */
    @RequestMapping(method = RequestMethod.GET, value = "{upc}")
    @ApiOperation("Returns a selling unit linked to the given upc.")
    @ApiResponses({
            @ApiResponse(code = 200, message = "upc\" : {upc}", response = Upc.class),
            @ApiResponse(code = 404, message = "error : UPC not found.")})
    public Upc findUpcInfoByUpc(
            @ApiParam(value = "The UPC code being requested.", required = true)
            @PathVariable("upc") Long upc, HttpServletRequest request) {

        UpcController.logger.info(String.format(UpcController.FIND_UPC, this.clientInfoService.getClientApplicationName(),
                request.getRemoteAddr(), upc));

        return this.upcService.getUpc(upc).orElseThrow(NotFoundException.NOT_FOUND_EXCEPTION_SUPPLIER);
    }

    /**
     * Searches for a list of upcs. Any upc not found in the list will not be returned. If no upc are
     * found, then the response is an empty list.
     *
     * @param sellingUnitSearchCriteria The search criteria to use when looking for upcs.
     * @param request The HTTP request that initiated the call.
     * @return A list of Upcs with the requested upcs.
     */
    @RequestMapping(method = RequestMethod.POST, value = BY_SEARCH_CRITERIA, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Searches for a collection of item codes based on search criteria.", httpMethod = "POST")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "A list of upcs that match the search criteria.", response = Upc.class),
            @ApiResponse(code = 400, message = "Search criteria invalid.")
    })
    public PageableResult findUpcsBySearchCriteria(@ApiParam(value = "A search criteria object to use to find upcs with.",
            example = "{ \"upcs\": [4122074262,4122006897,2200001857,412231001]}")
                                                   @RequestBody ExtendedSellingUnitSearchCriteria sellingUnitSearchCriteria, HttpServletRequest request) {

        UpcController.logger.info(String.format("Application %s from IP %s has requested information for the following search criteria: %s",
                this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), sellingUnitSearchCriteria.toString()));

        List<Upc> upcs = this.upcService.getUpcsBySearchCriteria(sellingUnitSearchCriteria);
        long totalRecordCount = upcs.size();
        return PageableResult.of(1, 1, totalRecordCount, upcs);
    }

    /**
     * Searches for a list of upcs. Any upc not found in the list will not be returned. This endpoint will stream
     * data as it becomes available rather than generate the full list as one message.
     *
     * @param sellingUnitSearchCriteria The search criteria to use when looking for upcs.
     * @param request The HTTP request that initiated the call.
     * @param response The HTTP response to write the upcs to.
     * @throws IOException
     */
    @RequestMapping(method = RequestMethod.POST, value = BY_SEARCH_CRITERIA_STREAMING, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Searches for a collection of upcs based on search criteria. This operation will stream" +
            "the data as it becomes available.	", httpMethod = "POST")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "A list of upcs that match the search criteria.", response = Upc.class),
            @ApiResponse(code = 400, message = "Search criteria invalid.")
    })
    public void streamUpcsBySearchCriteria(@ApiParam(value = "A search criteria object to use to find upcs with.",
            example = "{ \"upcs\": [4122074262,4122006897,2200001857,412231001]}")
                                            @RequestBody ExtendedSellingUnitSearchCriteria sellingUnitSearchCriteria, HttpServletRequest request,
                                            HttpServletResponse response) throws IOException {

        UpcController.logger.info(String.format("Application %s from IP %s has requested to stream information for the following search criteria: %s",
                this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), sellingUnitSearchCriteria.toString()));

        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        this.upcService.streamUpcsBySearchCriteria(sellingUnitSearchCriteria, response.getOutputStream());

    }

	/**
	 * Generates a new UPC.
	 *
	 * @return the new UPC
	 */
	@RequestMapping(method = RequestMethod.POST, value = NEW_UPC, consumes = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation("Generates a new UPC.")
	@ApiResponses({
			@ApiResponse(code = 200, message = "A newly generated UPC of requested type", response = Long.class),
			@ApiResponse(code = 400, message = "Body must include all fields in NewUpcRequest"),
	})
	@PreAuthorize("hasAuthority('GENERATE_UPC')")
	public Long generateUPC(@RequestBody NewUpcRequest newUpcRequest) {

		if (Objects.isNull(newUpcRequest) ||
				Objects.isNull(newUpcRequest.getUserId()) ||
				Objects.isNull(newUpcRequest.getRequestType())) {
			throw new InvalidRequestException("UserId and RequestType must be supplied.");
		}

		UpcController.logger.info(String.format(UpcController.NEW_UPC_MESSAGE, newUpcRequest.getUserId(),
				newUpcRequest.getRequestType()));

		return this.upcService.generateUPC(newUpcRequest.getRequestType());
	}


	/**
	 * Processes a collection of UpcMaintenanceRequests.
	 *
	 * @param maintenanceRequests The list of UpcMaintenanceRequests to process.
	 * @param request The HttpServletRequest that initiated this call.
	 * @return The updated Upcs.
	 */
	@RequestMapping(method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation("Updates a collection of UPCs.")
	@ApiResponses({
			@ApiResponse(code = 200, message = "The updted UPCs.", response = Upc.class),
			@ApiResponse(code = 409, message = "The request fails validation"),
	})
	@PreAuthorize("hasAuthority('UPDATE_PRODUCT')")
	public List<Upc> updateUpcs(@RequestBody List<UpcMaintenanceRequest> maintenanceRequests,
										  HttpServletRequest request) {

		String requestsAsString = ListUtils.toString(maintenanceRequests);

		UpcController.logger.info(String.format("Application %s from IP %s has requested to update the lifestyle claims of multiple UPCS: '%s'.",
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), requestsAsString));

		// Update the UPCs.
		this.upcMaintenanceService.updateUpcs(maintenanceRequests);

		// Go and grab the updated UPCs and return them.
		return this.upcService.getUpcs(maintenanceRequests.stream().map(UpcMaintenanceRequest::getUpc).collect(Collectors.toList()));
	}

    /**
     *  Approve the Genesis nutrient candidate by work id.
     * @param workId Candidate word request number.
     * @param userId The user approved.
     * @param request The HTTP request that initiated the call.
     */
    @RequestMapping(method = RequestMethod.POST,  value = APPROVAL_NUTRITION)
    @ApiOperation(value = "Approval nutrition by work request number", httpMethod = "POST")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "True if approval success")
    })
    public void approveNutritionByWorkId(@RequestParam("workId") Long workId, @RequestParam("userId") String userId, HttpServletRequest request) {
        UpcController.logger.info(String.format("Application %s from IP %s has requested approval to work request id : %s by user : %s",
                this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), workId, userId));
        this.genesisApprovalService.approveNutritionByWorkId(workId, userId);

    }

}
