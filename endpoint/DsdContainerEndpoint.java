/*
 * DsdContainerEndpoint
 *
 * Copyright (c) 2020 HEB
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of HEB.
 */
package com.heb.pm.core.endpoint;

import com.heb.pm.core.model.dsdcontainer.CandidatePalletItemRequest;
import com.heb.pm.core.model.dsdcontainer.DsdAuthorizeItemRequest;
import com.heb.pm.core.model.dsdcontainer.DsdContainerRequest;
import com.heb.pm.core.exception.NotFoundException;
import com.heb.pm.core.model.dsdcontainer.CandidatePalletItem;
import com.heb.pm.core.service.dsdcontainer.DsdContainerService;
import com.heb.pm.core.service.dsdcontainer.DsdContainerValidator;
import com.heb.pm.dao.core.entity.CandidatePalletCandidateItem;
import com.heb.pm.util.endpoint.PageableResult;
import com.heb.pm.util.security.wsag.ClientInfoService;
import io.swagger.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.Objects;

/**
 * REST endpoint for Dsd Container information.
 *
 * @author vn73545
 * @since 1.32.0
 */
@RestController()
@RequestMapping("/dsdContainer")
@Api(value = "DSD CONTAINER API", produces = MediaType.APPLICATION_JSON_VALUE)
public class DsdContainerEndpoint {

    private static final Logger logger = LoggerFactory.getLogger(DsdContainerEndpoint.class);

    @Autowired
    private transient DsdContainerService dsdContainerService;

    @Autowired
    private transient ClientInfoService clientInfoService;

    @Autowired
    private transient DsdContainerValidator dsdContainerValidator;

    /**
     * Returns CandidatePalletCandidateItem information for a given id.
     *
     * @param id      The id being requested.
     * @param request The HTTP servlet request that initiated the request.
     * @return The CandidatePalletCandidateItem information for the requested id.
     */
    @RequestMapping(method = RequestMethod.GET, value = "/{id}")
    @ApiOperation("Returns CandidatePalletCandidateItem information linked to the given id.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "id\" : {id}", response = CandidatePalletCandidateItem.class),
            @ApiResponse(code = 404, message = "error : id not found.")})
    public CandidatePalletItem getCandidatePalletCandidateItemById(@ApiParam("The id being requested.")
                                                                   @PathVariable("id") Long id, HttpServletRequest request) {

        DsdContainerEndpoint.logger.info(String.format("Application %s from IP %s requested CandidatePalletCandidateItem information for %d.",
                this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), id));

        return this.dsdContainerService.getCandidatePalletCandidateItemById(id).orElseThrow(NotFoundException.NOT_FOUND_EXCEPTION_SUPPLIER);
    }

    /**
     * Update candidate pallet candidate item.
     *
     * @param candidatePalletItemRequest The candidate pallet item request.
     * @param request                    The http request.
     * @return The id of candidate pallet item updated.
     * @throws Exception The exception to throw back.
     */
    @PostMapping("/")
    @ApiOperation(value = "Updates CandidatePalletCandidateItem information.", httpMethod = "POST")
    @ApiResponses({
            @ApiResponse(code = 200, message = "palletOfferId", response = Long.class),
            @ApiResponse(code = 409, message = "Unable to validate CandidatePalletItem update request.")
    })
    public Long updateCandidatePalletCandidateItem(@RequestBody CandidatePalletItemRequest candidatePalletItemRequest, HttpServletRequest request) throws Exception {

        DsdContainerEndpoint.logger.info(String.format("Application %s from IP %s requested to update CandidatePalletCandidateItem information.",
                this.clientInfoService.getClientApplicationName(), request.getRemoteAddr()));

        this.dsdContainerValidator.validateCandidatePalletItemRequest(candidatePalletItemRequest);
        if (Objects.nonNull(candidatePalletItemRequest.getId()) && !candidatePalletItemRequest.getId().equals(0L)) {
            return this.dsdContainerService.updateCandidatePalletCandidateItem(candidatePalletItemRequest);
        }
        return this.dsdContainerService.addNewCandidatePalletCandidateItem(candidatePalletItemRequest);
    }

    /**
     * Submit DSD container item.
     *
     * @param dsdContainerRequest The DSD container request.
     * @param request             The http request.
     * @throws Exception The exception to throw back.
     */
    @PostMapping("/submit")
    @ApiOperation(value = "Submit DSD by Candidate pallet id.", httpMethod = "POST")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "True if submit success")
    })
    public void submitDSDContainer(@RequestBody DsdContainerRequest dsdContainerRequest, HttpServletRequest request) throws Exception {
        logger.info("Application {} from IP {} has requested to submit DSD container setup with id: {} by user {}.",
                this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), dsdContainerRequest.getPalletId(), dsdContainerRequest.getUserId());
        this.dsdContainerService.submit(dsdContainerRequest.getPalletId(), dsdContainerRequest.getUserId());
    }

    /**
     * Approve DSD container item.
     *
     * @param dsdContainerRequest The DSD container request.
     * @param request             The http request.
     * @throws Exception The exception to throw back.
     */
    @PostMapping("/approve")
    @ApiOperation(value = "Approval DSD by Candidate pallet id.", httpMethod = "POST")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "True if approval success")
    })
    public void approveDSDContainer(@RequestBody DsdContainerRequest dsdContainerRequest, HttpServletRequest request) throws Exception {
        logger.info("Application {} from IP {} has requested to approve DSD container setup with id: {} by user {}.",
                this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), dsdContainerRequest.getPalletId(), dsdContainerRequest.getUserId());
        this.dsdContainerService.approve(dsdContainerRequest.getPalletId(), dsdContainerRequest.getUserId());
    }

    /**
     * Find candidate pallet candidate items by search criteria.
     *
     * @param dsdContainerRequest The DSD container request.
     * @param request             The http request.
     * @return The pageable result of candidate pallet candidate items.
     */
    @RequestMapping(method = RequestMethod.POST, value = "/search", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ApiOperation(value = "Searches for a collection of CandidatePalletItem based on search criteria.", httpMethod = "POST")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "A list of CandidatePalletItem that match the search criteria.", response = CandidatePalletItem.class),
            @ApiResponse(code = 400, message = "Search criteria invalid.")
    })
    public PageableResult findBySearchCriteria(@ApiParam("A search criteria object to use to find CandidatePalletItems with.")
                                               @RequestBody DsdContainerRequest dsdContainerRequest, HttpServletRequest request) {

        logger.info(String.format("Application %s from IP %s has requested information for the following search criteria: %s",
                this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), dsdContainerRequest.toString()));
        return this.dsdContainerService.findBySearchCriteria(dsdContainerRequest);
    }

    /**
     * Authorize stores.
     *
     * @param dsdAuthorizeItemRequest The DSD authorize item request.
     * @param request                 The http request.
     * @return The container upc.
     * @throws Exception The exception to throw back.
     */
    @PostMapping("/authorStores")
    @ApiOperation("Authorize stores.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "True if author stores success")
    })
    public Long authorStores(@RequestBody DsdAuthorizeItemRequest dsdAuthorizeItemRequest, HttpServletRequest request) throws Exception {

        logger.info("Application {} from IP {} has requested authorizing stores by user {}",
                this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), dsdAuthorizeItemRequest.getUserId());
        this.dsdContainerValidator.validateAuthorStoresRequest(dsdAuthorizeItemRequest);
        return this.dsdContainerService.authorizeStore(dsdAuthorizeItemRequest);
    }

}
