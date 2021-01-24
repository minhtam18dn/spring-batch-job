/*
 * ItemController.java
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
import com.heb.pm.core.exception.ValidationException;
import com.heb.pm.core.maintenance.BaseUpcMaintenanceRequest;
import com.heb.pm.core.maintenance.UpdateVendorItemFactoryXrfRequest;
import com.heb.pm.core.model.Item;
import com.heb.pm.core.model.audit.ItemAudit;
import com.heb.pm.core.service.ItemAuditService;
import com.heb.pm.core.service.ItemService;
import com.heb.pm.core.service.maintenance.WarehouseItemMaintenanceService;
import com.heb.pm.core.service.maintenance.swap.ExtendedAddAssociateRequest;
import com.heb.pm.core.service.maintenance.swap.ExtendedWarehouseMoveRequest;
import com.heb.pm.core.service.maintenance.swap.ExtendedWarehouseSwapRequest;
import com.heb.pm.core.service.maintenance.vendorItemFactory.VendorItemFactoryService;
import com.heb.pm.core.service.search.ExtendedItemSearchCriteria;
import com.heb.pm.dao.core.entity.codes.ItemKeyType;
import com.heb.pm.util.endpoint.PageableResult;
import com.heb.pm.util.security.wsag.ClientInfoService;
import io.swagger.annotations.*;
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

/**
 * Rest endpoint for finding item information.
 */
@RestController()
@RequestMapping(ItemEndpoint.ITEM_CODE_BASE_URL)
@Api(value = "Item API", produces = MediaType.APPLICATION_JSON_VALUE)
public class ItemEndpoint {

	private static final Logger logger = LoggerFactory.getLogger(ItemEndpoint.class);

	protected static final String ITEM_CODE_BASE_URL = "/item";

	private static final String BY_ITEM_CODE_URL = "{itemCode}";

	private static final String AUDIT_BY_ITEM_CODE_URL = "{itemCode}/audit";
	private static final String ADD_ASSOCIATE_URL = "{itemCode}/add";
	private static final String MOVE_URL = "{itemCode}/move";
	private static final String MOVE_AS_SUPPORT_URL = "{itemCode}/support/move";
	private static final String SWAP_URL = "{itemCode}/swap";
	private static final String SWAP_AS_SUPPORT_URL = "{itemCode}/support/swap";
	private static final String BY_SEARCH_CRITERIA = "/search";
	private static final String BY_SEARCH_CRITERIA_STREAMING = "/search/stream";
	private static final String UPDATE_VENDOR_ITEM_FACTORY = "{itemCode}/updateVendorItemFactory";

	private static final String FIND_ITEM_INFO_BY_ITEM_CD =
			"IP %s has requested item information for item code: %d";

	private static final String FIND_ITEM_AUDIT_INFO_BY_ITEM_CD =
			"IP %s has requested item audit information by item code: %d.";

	@Autowired
	private transient ItemService itemService;

	@Autowired
	private transient ItemAuditService itemAuditService;

	@Autowired
	private transient WarehouseItemMaintenanceService warehouseItemMaintenanceService;

	@Autowired
	private transient ClientInfoService clientInfoService;

	@Autowired
	private transient VendorItemFactoryService vendorItemFactoryService;

	/**
	 *  Returns warehouse item information for a given item code. This will set the HTTP status to 404 if not found.
	 *
	 * @param itemCode The item code being requested.
	 * @param request The HTTP servlet request that initiated the request.
	 * @return The item information for the requested item.
	 */
	@RequestMapping(method = RequestMethod.GET, value = BY_ITEM_CODE_URL)
	@ApiOperation("Returns warehouse item information linked to the given item code.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "itemCode\" : {itemCode}", response = Item.class),
			@ApiResponse(code = 404, message = "error : Item code not found.")})
	public Item getItemInfo(
			@ApiParam("The item code being requested.")
			@PathVariable("itemCode") Long itemCode,
			HttpServletRequest request) {

		ItemEndpoint.logger.info(String.format(ItemEndpoint.FIND_ITEM_INFO_BY_ITEM_CD, request.getRemoteAddr(), itemCode));

		return this.itemService.getItem(itemCode).orElseThrow(NotFoundException.NOT_FOUND_EXCEPTION_SUPPLIER);
	}

	/**
	 * Moves UPCs from one item code to another.
	 *
	 * @param itemCode The item code to move the UPCs to.
	 * @param warehouseMoveRequest The request with the UPCs to move.
	 * @param request The HTTP servlet request that triggered this call.
	 * @return The updated item.
	 */
	@ApiOperation("Moves UPCs from one item to another.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "The updated item.", response = Item.class),
			@ApiResponse(code = 409, message = "Invalid request."),
			@ApiResponse(code = 500, message = "Add associate operation failed.")})
	@PreAuthorize("hasAuthority('SWAP')")
	@RequestMapping(method = RequestMethod.POST, value = MOVE_URL)
	public Item moveUpcs(
			@ApiParam("The item code to add the UPCs to.")
			@PathVariable("itemCode") Long itemCode,
			@ApiParam("The request containing the UPCs to move.")
			@RequestBody ExtendedWarehouseMoveRequest warehouseMoveRequest,
			HttpServletRequest request) {

		if (Objects.nonNull(warehouseMoveRequest.getRunId())) {
			throw new ValidationException("Unable to perform move operation.", List.of("This endpoint cannot be used to reprocess swap requests."));
		}

		warehouseMoveRequest.setDestinationItemId(itemCode);

		return this.moveUpcs(warehouseMoveRequest, request);
	}

	/**
	 * Moves UPCs from one item code to another. This endpoint required an already processed request
	 * as it is used to re-run a swap after a failure.
	 *
	 * @param itemCode The item code to move the UPCs to.
	 * @param warehouseMoveRequest The request with the UPCs to move.
	 * @param request The HTTP servlet request that triggered this call.
	 * @return The updated item.
	 */
	@ApiOperation(value = "Moves UPCs from one item to another. This endpoint is for production support.", hidden = true)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "The updated item.", response = Item.class),
			@ApiResponse(code = 409, message = "Invalid request."),
			@ApiResponse(code = 500, message = "Add associate operation failed.")})
	@PreAuthorize("hasAuthority('REPLAY_SWAP')")
	@RequestMapping(method = RequestMethod.POST, value = MOVE_AS_SUPPORT_URL)
	public Item moveUpcsAsSupport(
			@ApiParam("The item code to add the UPCs to.")
			@PathVariable("itemCode") Long itemCode,
			@ApiParam("The request containing the UPCs to move.")
			@RequestBody ExtendedWarehouseMoveRequest warehouseMoveRequest,
			HttpServletRequest request) {

		this.validateReprocess(warehouseMoveRequest, itemCode, warehouseMoveRequest.getDestinationItemId());

		return this.moveUpcs(warehouseMoveRequest, request);
	}

	private void validateReprocess(BaseUpcMaintenanceRequest upcMaintenanceRequest, Long pathItem, Long requestItem) {

		if (Objects.isNull(upcMaintenanceRequest.getRunId())) {
			throw new ValidationException("Unable to perform operation.", List.of("This endpoint requires an already processed swap requests."));
		}

		if (!Objects.equals(pathItem, requestItem)) {
			throw new ValidationException("Unable to perform operation.", List.of("The path item and the request item must match."));
		}
	}

	private Item moveUpcs(ExtendedWarehouseMoveRequest warehouseMoveRequest, HttpServletRequest request) {

		if (Objects.isNull(warehouseMoveRequest.getUserId())) {
			throw new ValidationException("Unable to move UPCs.", List.of("User ID is required."));
		}

		ItemEndpoint.logger.info(String.format("User %s using application %s from IP %s requested to move UPCs: '%s'.",
				warehouseMoveRequest.getUserId(), this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), warehouseMoveRequest.toString()));

		this.warehouseItemMaintenanceService.moveUpcs(warehouseMoveRequest);

		// Return the updated item. The new associate will be there.
		return this.itemService.getItem(warehouseMoveRequest.getDestinationItemId()).orElseThrow(NotFoundException.NOT_FOUND_EXCEPTION_SUPPLIER);
	}

	/**
	 * Swaps the UPCs tied to this item with those tied to another.
	 *
	 * @param itemCode The item code to move the UPCs to.
	 * @param warehouseSwapRequest The request with the item to swap UPCs with.
	 * @param request The HTTP servlet request that triggered this call.
	 * @return The updated item.
	 */
	@ApiOperation("Swaps UPCs from one item to another.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "The updated item.", response = Item.class),
			@ApiResponse(code = 409, message = "Invalid request."),
			@ApiResponse(code = 500, message = "Add associate operation failed.")})
	@PreAuthorize("hasAuthority('SWAP')")
	@RequestMapping(method = RequestMethod.POST, value = SWAP_URL)
	public Item swapUpcs(
			@ApiParam("One side of the swap request. They are effectively the same, but this item's data will be returned.")
			@PathVariable("itemCode") Long itemCode,
			@ApiParam("The request containing the item to swap UPCs with.")
			@RequestBody ExtendedWarehouseSwapRequest warehouseSwapRequest,
			HttpServletRequest request) {

		if (Objects.nonNull(warehouseSwapRequest.getRunId())) {
			throw new ValidationException("Unable to perform move operation.", List.of("This endpoint cannot be used to reprocess swap requests."));
		}

		warehouseSwapRequest.setDestinationItemId(itemCode);

		return this.doSwap(warehouseSwapRequest, request);
	}

	/**
	 * Swaps the UPCs tied to this item with those tied to another. This endpoint requires an already processed
	 * swap request.
	 *
	 * @param itemCode The item code to move the UPCs to.
	 * @param warehouseSwapRequest The request with the item to swap UPCs with.
	 * @param request The HTTP servlet request that triggered this call.
	 * @return The updated item.
	 */
	@ApiOperation("Swaps UPCs from one item to another.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "The updated item.", response = Item.class),
			@ApiResponse(code = 409, message = "Invalid request."),
			@ApiResponse(code = 500, message = "Add associate operation failed.")})
	@PreAuthorize("hasAuthority('REPLAY_SWAP')")
	@RequestMapping(method = RequestMethod.POST, value = SWAP_AS_SUPPORT_URL)
	public Item swapUpcsAsSupport(
			@ApiParam("One side of the swap request. They are effectively the same, but this item's data will be returned.")
			@PathVariable("itemCode") Long itemCode,
			@ApiParam("The request containing the item to swap UPCs with.")
			@RequestBody ExtendedWarehouseSwapRequest warehouseSwapRequest,
			HttpServletRequest request) {

		this.validateReprocess(warehouseSwapRequest, itemCode, warehouseSwapRequest.getDestinationItemId());

		return this.doSwap(warehouseSwapRequest, request);
	}

	/**
	 * Calls the service to kick off the swap operation.
	 *
	 * @param warehouseSwapRequest The ExtendedWarehouseSwapRequest to execute.
	 * @param request The HTTP servlet request that triggered this call.
	 * @return The updated item.
	 */
	private Item doSwap(ExtendedWarehouseSwapRequest warehouseSwapRequest, HttpServletRequest request) {

		if (Objects.isNull(warehouseSwapRequest.getUserId())) {
			throw new ValidationException("Unable to swap UPCs.", List.of("User ID is required."));
		}

		ItemEndpoint.logger.info(String.format("User %s using application %s from IP %s requested to swap UPCs: '%s'.",
				warehouseSwapRequest.getUserId(), this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), warehouseSwapRequest.toString()));

		this.warehouseItemMaintenanceService.swapUpcs(warehouseSwapRequest);

		// Return the updated item. The new associate will be there.
		return this.itemService.getItem(warehouseSwapRequest.getDestinationItemId()).orElseThrow(NotFoundException.NOT_FOUND_EXCEPTION_SUPPLIER);
	}


	/**
	 * Adds a new associate UPC to an item code.
	 *
	 * @param itemCode The item code to add an associate UPC to.
	 * @param addAssociateRequest The UPC to add.
	 * @param request The HTTP servlet reqeust that triggerd this call.
	 * @return The updated item.
	 */
	@ApiOperation("Adds an associate UPC to an item.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "The updated item.", response = Item.class),
			@ApiResponse(code = 409, message = "Invalid request."),
			@ApiResponse(code = 500, message = "Add associate operation failed.")})
	@PreAuthorize("hasAuthority('SWAP')")
	@RequestMapping(method = RequestMethod.POST, value = ADD_ASSOCIATE_URL)
	public Item addAssociate(
			@ApiParam("The item code to add an associate to.")
			@PathVariable("itemCode") Long itemCode,
			@ApiParam("The add associate request.")
			@RequestBody ExtendedAddAssociateRequest addAssociateRequest,
			HttpServletRequest request) {

		if (Objects.isNull(addAssociateRequest.getUserId())) {
			throw new ValidationException("Unable to add associate UPC.", List.of("User ID is required."));
		}

		addAssociateRequest.setItemCode(itemCode);

		ItemEndpoint.logger.info(String.format("User %s using application %s from IP %s requested to add an associate UPC: '%s'.",
				addAssociateRequest.getUserId(), this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), addAssociateRequest.toString()));

		this.warehouseItemMaintenanceService.addAssociate(addAssociateRequest);

		// Return the updated item. The new associate will be there.
		return this.itemService.getItem(addAssociateRequest.getItemCode()).orElseThrow(NotFoundException.NOT_FOUND_EXCEPTION_SUPPLIER);
	}

	/**
	 * Finds warehouse item audit by item code. If there are no item master audits related to the given item code,
	 * this method will return a response code of 404. Else return the item master audit.
	 *
	 * @param itemCode Item id to search for.
	 * @param request The Http request.
	 * @param response The Http response.
	 * @return Item Audit related to given id and type or null if no item master audits are found.
	 */
	@RequestMapping(method = RequestMethod.GET, value = AUDIT_BY_ITEM_CODE_URL)
	@ApiOperation("Returns warehouse item audit information linked to the given item code.")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "itemCode\" : {itemCode}", response = Item.class),
			@ApiResponse(code = 404, message = "error : Item code not found.")})
	public ItemAudit getItemAuditInfo(
			@ApiParam("The item code for the requested item audit.")
			@PathVariable("itemCode") Long itemCode,

			@ApiParam("Filter names of the fields that the user wants removed from results. " +
					"If no filters are included, then all fields are " +
					"returned. If any are included, only the requested fields are returned." +
					" E.G. 'HIVEMIND'")
			@RequestParam(value = "filters", required = false)
					List<String> filters,

			HttpServletRequest request, HttpServletResponse response) {

		ItemEndpoint.logger.info(String.format(ItemEndpoint.FIND_ITEM_AUDIT_INFO_BY_ITEM_CD, request.getRemoteAddr(), itemCode));

		// Hardcoded to warehouse till service is updated to handle DSD/BOTH.
		ItemAudit itemAudit = this.itemAuditService.findByItemIdAndItemType(itemCode, ItemKeyType.WAREHOUSE.getId(), filters);
		if (itemAudit == null) {
			NotFoundException.NOT_FOUND_EXCEPTION_SUPPLIER.get();
		}
		return itemAudit;
	}

	/**
	 * Searches for a list of Items. Any item not found in the list will not be returned. If no item are
	 * found, then the response is an empty list.
	 *
	 * @param itemSearchCriteria The search criteria to use when looking for items.
	 * @param request The HTTP request that initiated the call.
	 * @return A list of Items with the requested IDs.
	 */
	@RequestMapping(method = RequestMethod.POST, value = BY_SEARCH_CRITERIA, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Searches for a collection of item codes based on search criteria.", httpMethod = "POST")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "A list of items that match the search criteria.", response = Item.class),
			@ApiResponse(code = 400, message = "Search criteria invalid.")
	})
	public PageableResult findItemBySearchCriteria(@ApiParam(value = "A search criteria object to use to find items with.",
			example = "{ \"itemCodes\": [146731,147709,207877,742627,836338,906716]}")
													  @RequestBody ExtendedItemSearchCriteria itemSearchCriteria, HttpServletRequest request) {

		ItemEndpoint.logger.info(String.format("Application %s from IP %s has requested information for the following search criteria: %s",
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), itemSearchCriteria.toString()));

		List<Item> items = this.itemService.getItemsBySearchCriteria(itemSearchCriteria);
		long totalRecordCount = items.size();
		return PageableResult.of(1, 1, totalRecordCount, items);
	}

	/**
	 * Searches for a list of items. Any item not found in the list will not be returned. This endpoint will stream
	 * data as it becomes available rather than generate the full list as one message.
	 *
	 * @param itemSearchCriteria The search criteria to use when looking for items.
	 * @param request The HTTP request that initiated the call.
	 * @param response The HTTP response to write the items to.
	 * @throws IOException
	 */
	@RequestMapping(method = RequestMethod.POST, value = BY_SEARCH_CRITERIA_STREAMING, consumes = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Searches for a collection of item IDs based on search criteria. This operation will stream" +
			"the data as it becomes available.	", httpMethod = "POST")
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "A list of items that match the search criteria.", response = Item.class),
			@ApiResponse(code = 400, message = "Search criteria invalid.")
	})
	public void streamItemsBySearchCriteria(@ApiParam(value = "A search criteria object to use to find items with.",
			example = "{ \"itemCodes\": [146731,147709,207877,742627,836338,906716]}")
											   @RequestBody ExtendedItemSearchCriteria itemSearchCriteria, HttpServletRequest request,
											HttpServletResponse response) throws IOException {

		ItemEndpoint.logger.info(String.format("Application %s from IP %s has requested to stream information for the following search criteria: %s",
				this.clientInfoService.getClientApplicationName(), request.getRemoteAddr(), itemSearchCriteria.toString()));

		response.setContentType(MediaType.APPLICATION_JSON_VALUE);
		this.itemService.streamItemsBySearchCriteria(itemSearchCriteria, response.getOutputStream());

	}

	@RequestMapping(method = RequestMethod.POST, value = UPDATE_VENDOR_ITEM_FACTORY)
	@ApiOperation("Update factory for item .")
	@ApiResponses({
			@ApiResponse(code = 200, message = "The original update vendor item factory request.", response = UpdateVendorItemFactoryXrfRequest.class),
			@ApiResponse(code = 400, message = "Vendor Item factory request invalid.")
	})
	public UpdateVendorItemFactoryXrfRequest massUpdate(
			@ApiParam("The item code to update vendor item factory.")
			@PathVariable("itemCode") Long itemCode,
			@ApiParam("The upadte vendor item factory request.")
			@RequestBody UpdateVendorItemFactoryXrfRequest vendorItemFactoryXrfRequest, HttpServletRequest request) {
		vendorItemFactoryXrfRequest.setItemCode(itemCode);
		validateVendorItemFactoryRequest(vendorItemFactoryXrfRequest);
		logger.info(String.format("User %s from IP %s update vendor item factory for item: %s.",
				vendorItemFactoryXrfRequest.getUserId(), request.getRemoteAddr(), vendorItemFactoryXrfRequest.getItemCode()));
		vendorItemFactoryService.updateVendorItemFactory(vendorItemFactoryXrfRequest);
		return vendorItemFactoryXrfRequest;
	}

	/**
	 * Checks to see if an vendor item factory request is valid. Will throw an exception if it is not.
	 *
	 * @param vendorItemFactoryXrfRequest The update vendor item factory request to check.
	 */
	private static void validateVendorItemFactoryRequest(UpdateVendorItemFactoryXrfRequest vendorItemFactoryXrfRequest) {

		if (Objects.isNull(vendorItemFactoryXrfRequest.getItemCode())) {
			throw new ValidationException("Unable to perform update vendor item factory", "Item code is required.");
		}

		if (Objects.isNull(vendorItemFactoryXrfRequest.getItemKeyTypeCode())) {
			throw new ValidationException("Unable to perform update vendor item factory", "Item key type code is required.");
		}

		if (Objects.isNull(vendorItemFactoryXrfRequest.getVendorLocationNumber())) {
			throw new ValidationException("Unable to perform update vendor item factory", "Vendor number is required.");
		}

		if (Objects.isNull(vendorItemFactoryXrfRequest.getVendorLocationTypeCode())) {
			throw new ValidationException("Unable to perform update vendor item factory", "Vendor Location Type is required.");
		}

		if (Objects.isNull(vendorItemFactoryXrfRequest.getUserId())) {
			throw new ValidationException("Unable to perform update vendor item factory", "User ID is required.");
		}
	}
}
