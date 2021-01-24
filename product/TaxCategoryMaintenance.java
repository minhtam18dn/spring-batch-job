package com.heb.pm.core.service.maintenance.product;

import com.heb.pm.core.exception.ValidationException;
import com.heb.pm.core.maintenance.ProductMaintenanceRequest;
import com.heb.pm.core.service.maintenance.common.MaintenanceConfig;
import com.heb.pm.dao.core.entity.GoodsProduct;
import com.heb.pm.dao.core.rowmappers.SingleRowResultSetExtractor;
import com.heb.pm.util.JdbcUtils;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nonnull;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import static com.heb.pm.util.ValidatorUtils.notNullOrIllegalArgument;

/**
 * Handles maintenance of the Vertex tax category attribute.
 *
 * @author d116773
 * @since 1.24.0
 */
public class TaxCategoryMaintenance {

	private static final String TAX_CATEGORY_UPDATE_SQL = "UPDATE EMD.GOODS_PROD " +
			"SET VERTEX_TAX_CAT_CD = ?, LST_UPDT_UID = ?, LST_UPDT_TS = ?, LST_SYS_UPDT_ID = ? " +
			"WHERE PROD_ID = ?";
	private static final String TAX_CATEGORY_SELECT_SQL = "SELECT VERTEX_TAX_CAT_CD, PROD_ID FROM EMD.GOODS_PROD WHERE PROD_ID = ?";

	private static final RowMapper<GoodsProduct> TAX_CATEGORY_ROW_MAPPER = (rs, rowNum) ->
			new GoodsProduct().setProdId(rs.getLong("PROD_ID"))
					.setVertexTaxCategoryCode(rs.getString("VERTEX_TAX_CAT_CD"));

	private final transient MaintenanceConfig config;

	/**
	 * BatchPreparedStatementSetter for handling updating the Vertex tax category.
	 *
	 * @author d116773
	 * @since 1.24.0
	 */
	private static class TaxCategorySetter extends BaseGoodsProductSetter {

		private TaxCategorySetter(Collection<GoodsProduct> products) {
			super(products);
		}

		@Override
		protected int setProductValues(@Nonnull PreparedStatement ps, @Nonnull GoodsProduct goodsProduct) throws SQLException {

			ps.setString(1, goodsProduct.getVertexTaxCategoryCode());
			return 1;
		}
	}

	/**
	 * Constructs a new TaxCategoryMaintenance object.
	 *
	 * @param maintenanceConfig The configuration for this class to use.
	 */
	public TaxCategoryMaintenance(MaintenanceConfig maintenanceConfig) {
		this.config = maintenanceConfig;
	}

	/**
	 * Processes a ProductMaintenanceRequest for a single product. If the Vertex tax category is present, it will
	 * update the product accordingly.
	 *
	 * @param productId                 The ID of the product to do the update on.
	 * @param productMaintenanceRequest The ProductMaintenanceRequest to process.
	 */
	@Transactional
	public void handleTaxCategory(long productId, ProductMaintenanceRequest productMaintenanceRequest) {

		if (Objects.nonNull(productMaintenanceRequest.getVertexTaxCategory())) {

			this.config.getLogger().info(String.format("Updating tax category of product %d to '%s'.",
					productId, productMaintenanceRequest.getVertexTaxCategory()));

			GoodsProduct toUpdate = new GoodsProduct().setProdId(productId).setVertexTaxCategoryCode(productMaintenanceRequest.getVertexTaxCategory());
			this.setTaxCategory(List.of(toUpdate), productMaintenanceRequest.getUserId());
		}
	}

	/**
	 * Sets the Vertex tax category for a list of products.
	 *
	 * @param goodsProducts The list of GoodsProduct objects to update. They should have (at a minimum) product ID and
	 *                      Vertex tax category set.
	 * @param userId The ID of the user making changes.
	 * @return The number of products updated.
	 */
	@Transactional
	public int setTaxCategory(List<GoodsProduct> goodsProducts, String userId) {

		notNullOrIllegalArgument(userId, "User ID cannot be empty.");

		goodsProducts.forEach(this::validate);

		SingleRowResultSetExtractor<GoodsProduct> singleRowResultSetExtractor = new SingleRowResultSetExtractor<>(TAX_CATEGORY_ROW_MAPPER);

		ProductMaintenanceUtils utils = new ProductMaintenanceUtils(this.config, userId);

		return utils.doGoodsProductUpdate(goodsProducts,
				goodsProduct -> {
					// Make sure it's a real product.
					GoodsProduct goodsProductFromDb =
							this.config.getJdbcTemplate().query(TAX_CATEGORY_SELECT_SQL,
									JdbcUtils.argsAsArray(goodsProduct.getProdId()), singleRowResultSetExtractor);
					if (Objects.isNull(goodsProductFromDb)) {
						this.config.getLogger().info("Attempt to update Vertex tax flag of {}, but it does not exist.", goodsProduct.getProdId());
						return Optional.empty();
					}

					// See if tax category changed.
					if (!Objects.equals(goodsProduct.getVertexTaxCategoryCode(), goodsProductFromDb.getVertexTaxCategoryCode())) {
						return Optional.of(goodsProduct);
					}
					return Optional.empty();
				},
				goodsProductsToUpdate -> {
					TaxCategorySetter taxCategorySetter = new TaxCategorySetter(goodsProductsToUpdate);
					int[] rowsUpdated = this.config.getJdbcTemplate().batchUpdate(TAX_CATEGORY_UPDATE_SQL, taxCategorySetter);
					this.config.getLogger().info(String.format("%,d tax category flags updated.", Arrays.stream(rowsUpdated).sum()));
				});
	}

	/**
	 * Validates a request to update tax category.
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
		if (Objects.isNull(goodsProduct.getVertexTaxCategoryCode())) {
			errors.add("Cannot set Vertex tax category to empty.");
		}

		if (!errors.isEmpty()) {
			throw new ValidationException("Unable to validate tax category update request.", errors);
		}
	}
}
