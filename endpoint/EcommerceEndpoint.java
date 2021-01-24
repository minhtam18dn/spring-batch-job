/*
 *  EcommerceEndpoint.java
 *
 *  Copyright (c) 2019 H-E-B
 *  All rights reserved.
 *
 *  This software is the confidential and proprietary information of H-E-B.
 */

package com.heb.pm.core.endpoint;

import com.heb.pm.core.ecommerce.EcommercePublicationRequest;
import com.heb.pm.core.exception.InvalidRequestException;
import com.heb.pm.core.service.ecommerce.EcommercePublicationService;
import com.heb.pm.util.ValidatorUtils;
import io.swagger.annotations.*;
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
 * REST endpoint for product publish related functions.
 *
 * @author vn73545
 * @since 1.14.0
 */
@RestController
@RequestMapping("/ecommerce")
@Api(value = "Ecommerce Endpoint",
        description = "Endpoint for eCommerce data",
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE)
public class EcommerceEndpoint {

    private static final Logger logger = LoggerFactory.getLogger(EcommerceEndpoint.class);

    @Autowired
    private transient EcommercePublicationService ecommercePublicationService;

    /**
     * Publish a product or a collection of products.
     *
     * @param ecommercePublicationRequest The request of what to publish. It should have either a product ID or a tracking ID set.
     * @param request                     The HTTP Servlet request that initiated this response.
     * @return The text OK if the publish product was successful.
     */
    @PostMapping("/publish")
    @ApiOperation("Publishes products to downstream eCommerce systems.")
    @ApiResponses({
            @ApiResponse(code = 200, message = "Product was published."),
            @ApiResponse(code = 400, message = "Publish Product request invalid."),
            @ApiResponse(code = 409, message = "Unable to validate published product request.")
    })
    @PreAuthorize("hasAuthority('ECOMMERCE_PUBLISH')")
    public String publish(@ApiParam(value = "The published product request.", required = true)
                          @RequestBody EcommercePublicationRequest ecommercePublicationRequest, HttpServletRequest request) {

        validateRequest(ecommercePublicationRequest);

        logger.info(String.format("User %s from IP %s has issued the following publish request: %s.",
                ecommercePublicationRequest.getUserId(), request.getRemoteAddr(), ecommercePublicationRequest));

        // If they've provided a tracking ID, publish all the requests that are part of that.
        if (Objects.nonNull(ecommercePublicationRequest.getTrackingId())) {
            return this.ecommercePublicationService.publishProductsByTrackingId(ecommercePublicationRequest.getTrackingId(), ecommercePublicationRequest.getUserId(),
                    ecommercePublicationRequest.getRunAsynchronously());
        }

        // If not, then publish by product ID.
        return this.ecommercePublicationService.publishProductByProductId(ecommercePublicationRequest.getProductId(), ecommercePublicationRequest.getAlertId(),
                ecommercePublicationRequest.getTargetSourceSystem(), ecommercePublicationRequest.getSalesChannelCode(), ecommercePublicationRequest.getUserId());
    }

    /**
     * Checks to see if an publish request is valid. Will throw an exception if it is not.
     *
     * @param ecommercePublicationRequest The ecommerce publication request to check.
     */
    private static void validateRequest(EcommercePublicationRequest ecommercePublicationRequest) {

        // Either tracking ID or product ID must be specified, but you cant set both.
        if (ValidatorUtils.notExclusive(ecommercePublicationRequest.getTrackingId(), ecommercePublicationRequest.getProductId())) {
            throw new InvalidRequestException("Either tracking ID or product ID must be provided, not both.");
        }

        if (Objects.nonNull(ecommercePublicationRequest.getProductId())) {
            if (Objects.isNull(ecommercePublicationRequest.getSalesChannelCode())) {
                throw new InvalidRequestException("Sales channel code is required.");
            }
            if (Objects.isNull(ecommercePublicationRequest.getTargetSourceSystem())) {
                throw new InvalidRequestException("Target Source System is required.");
            }
        }
        // We need to know who kicked off publish product.
        if (Objects.isNull(ecommercePublicationRequest.getUserId())) {
            throw new InvalidRequestException("User ID is required.");
        }

        // If they didn't provide a value for runAsynchronously, the default is false.
        if (Objects.isNull(ecommercePublicationRequest.getRunAsynchronously())) {
            ecommercePublicationRequest.setRunAsynchronously(Boolean.FALSE);
        }
    }
}
