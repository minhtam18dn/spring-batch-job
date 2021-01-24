package com.heb.pm.core.service.maintenance.product;

import com.heb.pm.core.exception.ValidationException;
import com.heb.pm.core.maintenance.ProductMaintenanceRequest;
import com.heb.pm.core.model.Code;
import com.heb.pm.core.service.maintenance.common.MaintenanceConfig;
import com.heb.pm.core.service.maintenance.common.TableUpdateSets;
import com.heb.pm.dao.core.LegacyEventGenerator;
import com.heb.pm.dao.core.entity.LegacyEvent;
import com.heb.pm.dao.core.entity.ProductShippingExceptions;
import com.heb.pm.dao.core.entity.ProductShippingExceptionsKey;
import com.heb.pm.dao.core.entity.codes.LegacyEventFunction;
import com.heb.pm.dao.core.entity.codes.ProductShippingMethodTypeCode;
import com.heb.pm.dao.core.preparedstatementsetters.ProductShippingExceptionUpdater;
import com.heb.pm.dao.core.preparedstatementsetters.UpdateType;
import com.heb.pm.dao.core.quicklookup.ProductLookup;
import com.heb.pm.dao.core.rowmappers.ProductShippingMethodRowMapper;
import com.heb.pm.util.JdbcUtils;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Handles maintenance of the Shipping method attribute.
 *
 * @author vn87351
 * @since 4.13.1
 */
public class ShippingExceptionMaintenance {

    private static final String SELECT_BY_PROD_ID_SQL =
            ProductShippingMethodRowMapper.SELECT_SQL + "  WHERE PSE.PROD_ID = ?";

    private static final String SHIPPING_METHOD_CODE_VALIDATION_SQL =
            "SELECT CUST_SHPNG_METH_CD FROM EMD.cust_shpng_meth WHERE CUST_SHPNG_METH_CD = ?";


    private final transient MaintenanceConfig conf;
    private final transient ProductLookup productLookup;
    private static final ProductShippingMethodRowMapper ROW_MAPPER = new ProductShippingMethodRowMapper();

    public ShippingExceptionMaintenance(MaintenanceConfig conf) {
        this.conf = conf;
        this.productLookup = new ProductLookup(this.conf.getJdbcTemplate());
    }

    /**
     * Updates a Product's Shipping method for a list of requests. This method assumes the caller is update/delete records that should be applied
     * to the Product. As a consequence, it compares against the current DB and, if there are new ones, it adds them. If there
     * are any delete records then we are delete it.
     *
     * @param productMaintenanceRequests The list of ProductMaintenanceRequest to process.
     * @return The number of rows updated.
     */
    @Transactional
    public int updateShippingMethods(List<ProductMaintenanceRequest> productMaintenanceRequests) {

        this.conf.requireForProcessing();

        int rowsModified = this.addShippingExceptions(productMaintenanceRequests);
        rowsModified += this.removeShippingExceptions(productMaintenanceRequests);

        // If the product is modified, generate PRMM, PRM2 events
        if (rowsModified > 0) {

            List<LegacyEvent> legacyEvents = new ArrayList();
            legacyEvents.add(LegacyEventGenerator.generatePRM2(productMaintenanceRequests.get(0).getProductId(),
                    this.conf.getProgramName(),
                    productMaintenanceRequests.get(0).getUserId(),
                    LegacyEventFunction.UPDATE));

            legacyEvents.add(LegacyEventGenerator.generatePRMM(productMaintenanceRequests.get(0).getProductId(),
                    this.conf.getProgramName(),
                    productMaintenanceRequests.get(0).getUserId(),
                    LegacyEventFunction.UPDATE));

            if (!legacyEvents.isEmpty()) {
                this.conf.getLegacyEventProcessor().addAndFlush(legacyEvents);
            }
        }

        return rowsModified;
    }

    private int removeShippingExceptions(List<ProductMaintenanceRequest> productMaintenanceRequests) {

        List<ProductShippingExceptions> methodsToRemove = productMaintenanceRequests.stream()
                .map(u -> requestToShippingMethod(u, u::getCustomerShippingMethodsToRemove))
                .flatMap(List::stream)
                .collect(Collectors.toList());

        return this.removeShippingMethods(methodsToRemove);
    }

    private int addShippingExceptions(List<ProductMaintenanceRequest> productMaintenanceRequests) {

        List<ProductShippingExceptions> methodsToAdd = productMaintenanceRequests.stream()
                .map(u -> requestToShippingMethod(u, u::getCustomerShippingMethodsToAdd))
                .flatMap(List::stream)
                .collect(Collectors.toList());


        return this.addShippingMethods(methodsToAdd);
    }


    /**
     * Converts all the shipping method in a ProductMaintenanceRequest to a list of ProductShippingExceptions.
     *
     * @param productMaintenanceRequest The list of ProductMaintenanceRequest to convert.
     * @return A list of ProductShippingExceptions converted from ProductMaintenanceRequest.
     */
    private static List<ProductShippingExceptions> requestToShippingMethod(ProductMaintenanceRequest productMaintenanceRequest,
                                                                           Supplier<Set<Code>> methodsSupplier) {

        List<ProductShippingExceptions> methods = new LinkedList<>();
        methodsSupplier.get().stream()
                .map(c -> requestToMethods(productMaintenanceRequest.getProductId(), c, productMaintenanceRequest.getUserId()))
                .forEach(methods::add);
        return methods;
    }

    /**
     * Adds a set of ProductShippingExceptions.
     *
     * @param methods The ProductShippingExceptions to add.
     * @return The number of rows added.
     */
    @Transactional
    public int addShippingMethods(List<ProductShippingExceptions> methods) {

        if (methods.isEmpty()) {
            return 0;
        }

        TableUpdateSets<ProductShippingExceptions> toInsert = new TableUpdateSets<>();

        // Get the list of shipping methods that this product already has.
        // Find all the shipping methods that the PROD ID doesn't already have, validate them, and collect them into a list.
        methods.stream()
                .filter(c -> !this.isProductHasMethod(c))
                .map(this::validate)
                .forEach(toInsert.getInserts()::add);

        return this.handleAllChanges(toInsert);
    }

    /**
     * Removes a set of ProductShippingExceptions.
     *
     * @param methods The ProductShippingExceptions to remove.
     * @return The number of rows deleted.
     */
    @Transactional
    public int removeShippingMethods(List<ProductShippingExceptions> methods) {

        if (methods.isEmpty()) {
            return 0;
        }

        TableUpdateSets<ProductShippingExceptions> toDelete = new TableUpdateSets<>();

        // Find all the shipping method that the Product already has, validate them, and collect them into a list.
        methods.stream()
                .filter(this::isProductHasMethod)
                .forEach(toDelete.getDeletes()::add);

        return this.handleAllChanges(toDelete);
    }

    /**
     * Validates a ProductShippingExceptions.
     *
     * @param method The ProductShippingExceptions to validate.
     * @throws ValidationException Any error when validating the request.
     */
    public ProductShippingExceptions validate(ProductShippingExceptions method) {

        List<String> errors = new LinkedList<>();

        // The Product has to be present and a real Product.
        if (Objects.isNull(method.getKey().getProductId())) {
            errors.add("Product Id is required.");
        } else {
            if (!this.productLookup.isProductId(method.getKey().getProductId())) {
                errors.add(String.format("%d is not a valid Product.", method.getKey().getProductId()));
            }
        }

        // User ID is required.
        if (Objects.isNull(method.getLastUpdateUserId()) || Objects.isNull(method.getCreateId())) {
            errors.add("User ID is required.");
        }

        // Make sure all the shipping methods are in the DB.
        if (Objects.isNull(method.getKey().getCustomerShippingMethodCode()) || Objects.isNull(method.getKey().getCustomerShippingMethodCode())) {
            errors.add("Shipping Method code is required.");
        } else if (JdbcUtils.runCountQuery(this.conf.getJdbcTemplate(), SHIPPING_METHOD_CODE_VALIDATION_SQL,
                JdbcUtils.argsAsArray(method.getKey().getCustomerShippingMethodCode().getId())) == 0) {
            errors.add(String.format("%s is not a valid shipping method code.", method.getKey().getCustomerShippingMethodCode()));
        }

        if (!errors.isEmpty()) {
            throw new ValidationException("Unable to validate shipping method request.", errors);
        }

        return method;
    }

    private boolean isProductHasMethod(ProductShippingExceptions method) {

        List<ProductShippingExceptions> shippingExceptions =
                this.findAllShippingMethodForProduct(method.getKey().getProductId());

        return shippingExceptions.stream().anyMatch(c -> Objects.equals(c.getKey().getCustomerShippingMethodCode(),
                method.getKey().getCustomerShippingMethodCode()));
    }

    private List<ProductShippingExceptions> findAllShippingMethodForProduct(Long productId) {

        return this.conf.getJdbcTemplate().query(SELECT_BY_PROD_ID_SQL, JdbcUtils.argsAsArray(productId), ROW_MAPPER);
    }

    /**
     * Converts a single shipping method code to a ProductShippingExceptions.
     *
     * @param productId      The productId the shipping method is for.
     * @param shippingMethod The shipping method code.
     * @param userId         The user who kicked off the change.
     * @return The shipping method code converted to a ProductShippingExceptions.
     */
    private static ProductShippingExceptions requestToMethods(Long productId, Code shippingMethod, String userId) {

        ProductShippingMethodTypeCode shippingMethodTypeCode = ProductShippingMethodTypeCode.of(shippingMethod.getId()); // Adds padding to make it to characters wide.

        ProductShippingExceptionsKey key = new ProductShippingExceptionsKey()
                .setProductId(productId)
                .setCustomerShippingMethodCode(shippingMethodTypeCode);

        return new ProductShippingExceptions().setKey(key)
                .setCreateId(userId)
                .setLastUpdateUserId(userId);
    }

    private int handleAllChanges(TableUpdateSets<ProductShippingExceptions> updates) {

        List<LegacyEvent> legacyEvents = new LinkedList<>();

        // Perform delete request and add events
        int rowsDeleted = this.handleDeletes(updates.getDeletes());
        updates.getDeletes().stream()
                .map(dth -> LegacyEventGenerator.generatePSHE(dth.getKey().getProductId(),
                        dth.getKey().getCustomerShippingMethodCode(),
                        this.conf.getProgramName(),
                        dth.getLastUpdateUserId(), LegacyEventFunction.DELETE))
                .forEach(legacyEvents::add);

        // Perform insert request and add events
        int rowsInserted = this.handleInserts(updates.getInserts());
        updates.getInserts().stream()
                .map(dth -> LegacyEventGenerator.generatePSHE(dth.getKey().getProductId(),
                        dth.getKey().getCustomerShippingMethodCode(),
                        this.conf.getProgramName(),
                        dth.getLastUpdateUserId(), LegacyEventFunction.ADD))
                .forEach(legacyEvents::add);

        if (!legacyEvents.isEmpty()) {
            this.conf.getLegacyEventProcessor().addAndFlush(legacyEvents);
        }

        this.conf.getLogger().info(String.format("%d shipping methods added and %d removed.", rowsInserted, rowsDeleted));

        // There are no events for this table.
        return rowsDeleted + rowsInserted;
    }

    private int handleDeletes(Set<ProductShippingExceptions> toDelete) {

        if (toDelete.isEmpty()) {
            return 0;
        }

        ProductShippingExceptionUpdater updater = new ProductShippingExceptionUpdater(toDelete, UpdateType.DELETE);

        int[] rowsDeleted = this.conf.getJdbcTemplate().batchUpdate(ProductShippingExceptionUpdater.DELETE_SQL, updater);
        return Arrays.stream(rowsDeleted).sum();
    }

    /**
     * Performs the inserts into the table.
     *
     * @param toInsert The records to insert.
     * @return The number of rows affected.
     */
    private int handleInserts(Set<ProductShippingExceptions> toInsert) {

        if (toInsert.isEmpty()) {
            return 0;
        }

        ProductShippingExceptionUpdater updater = new ProductShippingExceptionUpdater(toInsert, UpdateType.INSERT);

        int[] rowsInserted = this.conf.getJdbcTemplate().batchUpdate(ProductShippingExceptionUpdater.INSERT_SQL, updater);
        return Arrays.stream(rowsInserted).sum();
    }
}
