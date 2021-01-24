package com.heb.pm.core.service.maintenance.product;

import com.heb.pm.core.service.maintenance.common.MaintenanceConfig;
import com.heb.pm.core.service.maintenance.common.TableUpdateSets;
import com.heb.pm.dao.core.entity.ProductDescription;
import com.heb.pm.dao.core.preparedstatementsetters.ProductDescriptionUpdater;
import com.heb.pm.dao.core.preparedstatementsetters.UpdateType;
import com.heb.pm.util.JdbcUtils;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.Set;

/**
 * Handles maintenance of the product descriptions.
 *
 * @author m314029
 * @since 1.25.0
 */
public class ProductDescriptionMaintenance {

    protected static final String DELETE_AUDIT_INSERT_SQL = "INSERT INTO EMD.PROD_DESC_TXT_AUD (PROD_ID, DES_TYP_CD, LANG_TYP_CD, " +
            "AUD_REC_CRE8_TS, ACT_CD, PROD_DES, LST_UPDT_UID) " +
            "SELECT PROD_ID, DES_TYP_CD, LANG_TYP_CD, SYSDATE, 'PURGE', PROD_DES, LST_UPDT_UID " +
            "FROM EMD.PROD_DESC_TXT WHERE PROD_ID = ? AND DES_TYP_CD = ? AND LANG_TYP_CD = ?";

    private final transient MaintenanceConfig config;

    /**
     * Constructs a new ProductDescriptionMaintenance object.
     *
     * @param maintenanceConfig The configuration for this class to use.
     */
    public ProductDescriptionMaintenance(MaintenanceConfig maintenanceConfig) {
        this.config = maintenanceConfig;
    }

    /**
     * Handles the actual inserting of records into product description.
     *
     * @param recordsToInsert The ProductDescription objects to insert.
     * @return Count of records inserted.
     */
    @Transactional
    protected int doInserts(Set<ProductDescription> recordsToInsert) {

        ProductDescriptionUpdater productDescriptionUpdater = new ProductDescriptionUpdater(recordsToInsert, UpdateType.INSERT);

        int[] count = this.config.getJdbcTemplate().batchUpdate(ProductDescriptionUpdater.INSERT_SQL, productDescriptionUpdater);
        return Arrays.stream(count).sum();
    }

    /**
     * Handles the actual updating of records in product description.
     *
     * @param recordsToUpdate The ProductDescription objects to update.
     * @return Count of records updated.
     */
    @Transactional
    protected int doUpdates(Set<ProductDescription> recordsToUpdate) {

        ProductDescriptionUpdater productDescriptionUpdater = new ProductDescriptionUpdater(recordsToUpdate, UpdateType.UPDATE);

        int[] count = this.config.getJdbcTemplate().batchUpdate(ProductDescriptionUpdater.UPDATE_SQL, productDescriptionUpdater);
        return Arrays.stream(count).sum();
    }

    /**
     * Handles the actual deleting of records from product description.
     *
     * @param recordsToDelete The ProductOnline objects to delete.
     * @return Count of records deleted.
     */
    @Transactional
    protected int doDeletes(Set<ProductDescription> recordsToDelete) {

        // The delete trigger is missing from product description, so we need to copy the stuff we're deleting into the audits table.
        recordsToDelete.forEach(d -> this.config.getJdbcTemplate().update(ProductDescriptionMaintenance.DELETE_AUDIT_INSERT_SQL,
                JdbcUtils.argsAsArray(d.getKey().getProductId(),
                        d.getKey().getDescriptionType().getId(),
                        d.getKey().getLanguageType())));

        // Now we can do the deletes.
        ProductDescriptionUpdater productDescriptionUpdater = new ProductDescriptionUpdater(recordsToDelete, UpdateType.DELETE);
        int[] count = this.config.getJdbcTemplate().batchUpdate(ProductDescriptionUpdater.DELETE_SQL, productDescriptionUpdater);
        return Arrays.stream(count).sum();
    }

    /**
     * Does the work of calling the insert, update, and delete functions for a collection of ProductDescription updates.
     *
     * @param recordsToUpdate The collection of changes to make.
     */
    @Transactional
    protected int handleAllChanges(TableUpdateSets<ProductDescription> recordsToUpdate) {

        int countOfInserts = 0;
        int countOfUpdates = 0;
        int countOfDeletes = 0;
        // Issue the updates.
        if (!recordsToUpdate.getUpdates().isEmpty()) {
            countOfUpdates = this.doUpdates(recordsToUpdate.getUpdates());
        }

        // Issue any deletes.
        if (!recordsToUpdate.getDeletes().isEmpty()) {
            countOfDeletes = this.doDeletes(recordsToUpdate.getDeletes());
        }

        // Issue any inserts
        if (!recordsToUpdate.getInserts().isEmpty()) {
            countOfInserts = this.doInserts(recordsToUpdate.getInserts());
        }
        this.config.getLogger().info("Performed {} inserts, {} updates, and {} deletes on PROD_DESC_TXT.",
                countOfInserts, countOfUpdates, countOfDeletes);
        return countOfInserts + countOfUpdates + countOfDeletes;
    }
}
