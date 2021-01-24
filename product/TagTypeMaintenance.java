package com.heb.pm.core.service.maintenance.product;

import com.heb.pm.core.exception.ValidationException;
import com.heb.pm.core.maintenance.ProductMaintenanceRequest;
import com.heb.pm.core.service.maintenance.common.MaintenanceConfig;
import com.heb.pm.dao.core.entity.GoodsProduct;
import com.heb.pm.dao.core.rowmappers.SingleRowResultSetExtractor;
import com.heb.pm.util.JdbcUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nonnull;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import static com.heb.pm.util.ValidatorUtils.notNullOrIllegalArgument;

/**
 * Handles maintenance of the tag type attribute.
 *
 * @author d116773
 * @since 1.24.0
 */
public class TagTypeMaintenance {

	private static final int TAG_TYPE_CODE_SIZE = 5;

	private static final String TAG_UPDATE_SQL = "UPDATE EMD.GOODS_PROD " +
			"SET TAG_TYP_CD = ?, LST_UPDT_UID = ?, LST_UPDT_TS = ?, LST_SYS_UPDT_ID = ? " +
			"WHERE PROD_ID = ?";
	private static final String TAG_SELECT_SQL = "SELECT TAG_TYP_CD, PROD_ID FROM EMD.GOODS_PROD WHERE PROD_ID = ?";

	private static final String TAG_VALIDATION_SQL = "SELECT TAG_TYP_CD FROM EMD.TAG_TYP WHERE TAG_TYP_CD = ?";

	private static final RowMapper<GoodsProduct> TAG_ROW_MAPPER = (rs, rowNum) ->
			new GoodsProduct().setProdId(rs.getLong("PROD_ID"))
					.setTagTypeCode(rs.getString("TAG_TYP_CD"));

	private final transient MaintenanceConfig config;

	/**
	 * BatchPreparedStatementSetter for handling updating the tag type.
	 *
	 * @author d116773
	 * @since 1.24.0
	 */
	private static class TagTypeSetter extends BaseGoodsProductSetter {

		private TagTypeSetter(Collection<GoodsProduct> products) {
			super(products);
		}

		@Override
		protected int setProductValues(@Nonnull PreparedStatement ps, @Nonnull GoodsProduct goodsProduct) throws SQLException {

			ps.setString(1, goodsProduct.getTagTypeCode());
			return 1;
		}
	}

	/**
	 * Constructs a new TagTypeMaintenance object.
	 *
	 * @param maintenanceConfig The configuration for this class to use.
	 */
	public TagTypeMaintenance(MaintenanceConfig maintenanceConfig) {
		this.config = maintenanceConfig;
	}

	/**
	 * Processes a ProductMaintenanceRequest for a single product. If the tag type is present, it will
	 * update the product accordingly.
	 *
	 * @param productId                 The ID of the product to do the update on.
	 * @param productMaintenanceRequest The ProductMaintenanceRequest to process.
	 */
	@Transactional
	public void handleTagMaintenance(long productId, ProductMaintenanceRequest productMaintenanceRequest) {

		if (Objects.nonNull(productMaintenanceRequest.getTagType())) {

			// The tag-type in the DB is a fixed-width string. Pad out whatever the user sent.
			String paddedTagType = StringUtils.rightPad(productMaintenanceRequest.getTagType(), TAG_TYPE_CODE_SIZE);

			this.config.getLogger().info(String.format("Updating tag type of product %d to '%s'.", productId, paddedTagType));

			GoodsProduct toUpdate = new GoodsProduct().setProdId(productId).setTagTypeCode(paddedTagType);
			this.setTagType(List.of(toUpdate), productMaintenanceRequest.getUserId());
		}
	}

	/**
	 * Sets the tag type for a list of products.
	 *
	 * @param goodsProducts The list of GoodsProduct objects to update. They should have (at a minimum) product ID and
	 *                     tag type set.
	 * @param userId The ID of the user making changes.
	 * @return The number of products updated.
	 */
	@Transactional
	public int setTagType(List<GoodsProduct> goodsProducts, String userId) {

		notNullOrIllegalArgument(userId, "User ID cannot be empty.");

		goodsProducts.forEach(this::validate);

		SingleRowResultSetExtractor<GoodsProduct> singleRowResultSetExtractor = new SingleRowResultSetExtractor<>(TAG_ROW_MAPPER);

		ProductMaintenanceUtils utils = new ProductMaintenanceUtils(this.config, userId);

		return utils.doGoodsProductUpdate(goodsProducts,
				goodsProduct -> {
					// Make sure it's a real product.
					GoodsProduct goodsProductFromDb =
							this.config.getJdbcTemplate().query(TAG_SELECT_SQL,
									JdbcUtils.argsAsArray(goodsProduct.getProdId()), singleRowResultSetExtractor);
					if (Objects.isNull(goodsProductFromDb)) {
						this.config.getLogger().info("Attempt to update tag type of {}, but it does not exist.", goodsProduct.getProdId());
						return Optional.empty();
					}

					// See if tag type.
					if (!Objects.equals(goodsProduct.getTagTypeCode(), goodsProductFromDb.getTagTypeCode())) {
						return Optional.of(goodsProduct);
					}
					return Optional.empty();
				},
				goodsProductsToUpdate -> {
					TagTypeSetter tagTypeSetter = new TagTypeSetter(goodsProductsToUpdate);
					int[] rowsUpdated = this.config.getJdbcTemplate().batchUpdate(TAG_UPDATE_SQL, tagTypeSetter);
					this.config.getLogger().info(String.format("%,d tag types updated.", Arrays.stream(rowsUpdated).sum()));
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
		if (Objects.isNull(goodsProduct.getTagTypeCode())) {
			errors.add("Cannot set tag type to nothing.");
		} else {

			// Make sure the tag type is valid.
			if (JdbcUtils.runCountQuery(this.config.getJdbcTemplate(), TAG_VALIDATION_SQL, JdbcUtils.argsAsArray(goodsProduct.getTagTypeCode())) == 0) {
				errors.add(String.format("'%s' is not a valid tag type.", goodsProduct.getTagTypeCode()));
			}
		}

		if (!errors.isEmpty()) {
			throw new ValidationException("Unable to validate tag type update request.", errors);
		}
	}
}
