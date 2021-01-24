package com.heb.pm.core.service.maintenance.product;

import com.heb.pm.core.exception.ValidationException;
import com.heb.pm.core.maintenance.ProductMaintenanceRequest;
import com.heb.pm.core.service.maintenance.common.MaintenanceConfig;
import com.heb.pm.dao.core.DatabaseConstants;
import com.heb.pm.dao.core.entity.GoodsProduct;
import com.heb.pm.dao.core.rowmappers.SingleRowResultSetExtractor;
import com.heb.pm.util.JdbcUtils;
import com.heb.pm.util.ValidatorUtils;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nonnull;
import java.sql.*;
import java.util.*;

import static com.heb.pm.util.ValidatorUtils.notNullOrIllegalArgument;

/**
 * Handles maintenance of the self-manufactured attribute.
 *
 * @author d116773
 * @since 1.24.0
 */
public class SelfManufacturedMaintenance {

	private static final String SELF_MANUFACTURED_UPDATE_SQL = "UPDATE EMD.GOODS_PROD " +
			"SET SELF_MFG_SW = ?, LST_UPDT_UID = ?, LST_UPDT_TS = ?, LST_SYS_UPDT_ID = ? " +
			"WHERE PROD_ID = ?";
	private static final String SELF_MANUFACTURED_SELECT_SQL = "SELECT SELF_MFG_SW, PROD_ID FROM EMD.GOODS_PROD WHERE PROD_ID = ?";

	private final transient MaintenanceConfig maintenanceConfig;

	private static final RowMapper<GoodsProduct> SELF_MANUFACTURED_ROW_MAPPER = (rs, rowNum) ->
		new GoodsProduct().setProdId(rs.getLong("PROD_ID"))
				.setSelfManufacturedSwitch(rs.getString("SELF_MFG_SW"));

	/**
	 * BatchPreparedStatementSetter for handling updating the self-manufactured flag.
	 *
	 * @author d116773
	 * @since 1.24.0
	 */
	private static class SelfManufacturedSetter extends BaseGoodsProductSetter {

		private SelfManufacturedSetter(Collection<GoodsProduct> products) {
			super(products);
		}

		@Override
		protected int setProductValues(@Nonnull PreparedStatement ps, @Nonnull GoodsProduct goodsProduct) throws SQLException {

			ps.setString(1, goodsProduct.getSelfManufacturedSwitch());
			return 1;
		}
	}

	/**
	 * Constructs a new SelfManufacturedMaintenance object.
	 *
	 * @param maintenanceConfig The configuration for this class to use.
	 */
	public SelfManufacturedMaintenance(MaintenanceConfig maintenanceConfig) {

		this.maintenanceConfig = maintenanceConfig;
	}

	/**
	 * Processes a ProductMaintenanceRequest for a single product. If the self-manufactured switch is present, it will
	 * update the product accordingly.
	 *
	 * @param productId The ID of the product to do the update on.
	 * @param productMaintenanceRequest The ProductMaintenanceRequest to process.
	 */
	@Transactional
	public void handleSelfManufactured(long productId, ProductMaintenanceRequest productMaintenanceRequest) {

		if (Objects.equals(Boolean.TRUE, productMaintenanceRequest.getSelfManufactured())) {

			this.maintenanceConfig.getLogger().info(String.format("Updating self-manufactured flag of product %d to '%s'.",
					productId, DatabaseConstants.YES));

			GoodsProduct goodsProduct = new GoodsProduct().setProdId(productId)
					.setSelfManufacturedSwitch(DatabaseConstants.YES);
			this.setSelfManufactured(List.of(goodsProduct), productMaintenanceRequest.getUserId());
		}

		if (Objects.equals(Boolean.FALSE, productMaintenanceRequest.getSelfManufactured())) {

			this.maintenanceConfig.getLogger().info(String.format("Updating self-manufactured flag of product %d to '%s'.",
					productId, DatabaseConstants.NO));

			GoodsProduct goodsProduct = new GoodsProduct().setProdId(productId)
					.setSelfManufacturedSwitch(DatabaseConstants.NO);
			this.setSelfManufactured(List.of(goodsProduct), productMaintenanceRequest.getUserId());
		}
	}

	/**
	 * Sets the self-manufactured switch for a list of products.
	 *
	 * @param goodsProducts The list of products to update. It should have, at a minimum, the product ID and the
	 *                      self-manufactured switch set.
	 * @param userId The ID of the user making the change.
	 * @return The number of records updated.
	 */
	@Transactional
	public int setSelfManufactured(List<GoodsProduct> goodsProducts, String userId) {

		// Validate the request.

		notNullOrIllegalArgument(userId, "User ID cannot be empty.");

		goodsProducts.forEach(this::validate);

		SingleRowResultSetExtractor<GoodsProduct> singleRowResultSetExtractor = new SingleRowResultSetExtractor<>(SELF_MANUFACTURED_ROW_MAPPER);

		ProductMaintenanceUtils utils = new ProductMaintenanceUtils(this.maintenanceConfig, userId);

		return utils.doGoodsProductUpdate(goodsProducts,
				goodsProduct -> {
					// The product should exist.
					GoodsProduct goodsProductFromDb =
							this.maintenanceConfig.getJdbcTemplate().query(SELF_MANUFACTURED_SELECT_SQL,
									JdbcUtils.argsAsArray(goodsProduct.getProdId()), singleRowResultSetExtractor);
					if (Objects.isNull(goodsProductFromDb)) {
						this.maintenanceConfig.getLogger().info("Attempt to update self manufactured flag of {}, but it does not exist.", goodsProduct.getProdId());
						return Optional.empty();
					}

					// See if the self-manufactured flag changed.
					if (!Objects.equals(goodsProduct.getSelfManufacturedSwitch(), goodsProductFromDb.getSelfManufacturedSwitch())) {
						return Optional.of(goodsProduct);
					}
					return Optional.empty();
				},
				goodsProductsToUpdate -> {
					SelfManufacturedSetter selfManufacturedSetter = new SelfManufacturedSetter(goodsProductsToUpdate);
					int[] rowsUpdated = this.maintenanceConfig.getJdbcTemplate().batchUpdate(SELF_MANUFACTURED_UPDATE_SQL, selfManufacturedSetter);
					this.maintenanceConfig.getLogger().info(String.format("%,d self manufactured flags updated.", Arrays.stream(rowsUpdated).sum()));
				});
	}

	/**
	 * Validates a request to change the self-manufactured flag.
	 *
	 * @param goodsProduct The GoodsProduct to validate.
	 */
	public void validate(GoodsProduct goodsProduct) {

		List<String> errors = new LinkedList<>();

		// Prod ID cannot be null.
		if (Objects.isNull(goodsProduct.getProdId())) {
			errors.add("Product ID cannot be empty.");
		}

		// Make sure the tax category has a value.
		if (Objects.isNull(goodsProduct.getSelfManufacturedSwitch())) {
			errors.add("Cannot set self-manufactured to empty.");
		} else {

			// Self-man has to be Y or N.
			ValidatorUtils.validateFieldInList(goodsProduct::getSelfManufacturedSwitch,
					"Self-manufactured must be Y or N.", DatabaseConstants.YES, DatabaseConstants.NO)
					.ifPresent(errors::add);
		}

		if (!errors.isEmpty()) {
			throw new ValidationException("Unable to validate request to change self-manufactured flag.", errors);
		}
	}
}
