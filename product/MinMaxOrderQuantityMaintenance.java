package com.heb.pm.core.service.maintenance.product;

import com.heb.pm.core.exception.ValidationException;
import com.heb.pm.core.maintenance.ProductMaintenanceRequest;
import com.heb.pm.core.model.DiscountThreshold;
import com.heb.pm.core.service.maintenance.common.MaintenanceConfig;
import com.heb.pm.core.service.maintenance.common.TableUpdateSets;
import com.heb.pm.dao.core.LegacyEventGenerator;
import com.heb.pm.dao.core.entity.GoodsProduct;
import com.heb.pm.dao.core.entity.LegacyEvent;
import com.heb.pm.dao.core.entity.ProductDiscountThreshold;
import com.heb.pm.dao.core.entity.ProductDiscountThresholdKey;
import com.heb.pm.dao.core.entity.codes.LegacyEventFunction;
import com.heb.pm.dao.core.preparedstatementsetters.ProductDiscountThresholdUpdater;
import com.heb.pm.dao.core.preparedstatementsetters.UpdateType;
import com.heb.pm.dao.core.quicklookup.ProductLookup;
import com.heb.pm.dao.core.rowmappers.ProductDiscountThresholdRowMapper;
import com.heb.pm.dao.core.rowmappers.SingleRowResultSetExtractor;
import com.heb.pm.dao.oracle.BigDecimalDoubleScaledConverter;
import com.heb.pm.util.DateUtils;
import com.heb.pm.util.InstantUtils;
import com.heb.pm.util.JdbcUtils;
import com.heb.pm.util.MappingUtils;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static com.heb.pm.util.ValidatorUtils.notNullOrIllegalArgument;
import static com.heb.pm.util.ValidatorUtils.validateFieldExists;

/**
 * Handles maintenance of the self-manufactured attribute.
 *
 * @author d116773
 * @since 1.24.0
 */
public class MinMaxOrderQuantityMaintenance {

	private static final BigDecimal MINIMUM_MINIMUM_ORDER_QUANTITY = BigDecimal.ZERO;
	private static final BigDecimal MAXIMUM_MAXIMUM_ORDER_QUANTITY = new BigDecimal("9999.00");
	private static final BigDecimal OLD_MAXIMUM_DEFAULT = new BigDecimal("999999999.00");
	private static final int DESIRED_MAXIMUM_DISCOUNT_THRESHOLDS_PER_PRODUCT = 5;

	private static final String MIN_MAX_UPDATE_SQL = "UPDATE EMD.GOODS_PROD " +
			"SET MIN_CUST_ORD_QTY = ?, MAX_CUST_ORD_QTY = ?, LST_UPDT_UID = ?, LST_UPDT_TS = ?, LST_SYS_UPDT_ID = ? " +
			"WHERE PROD_ID = ?";

	private static final String MIN_MAX_ORDER_QUANTITY_SELECT_SQL = "SELECT MIN_CUST_ORD_QTY, MAX_CUST_ORD_QTY, PROD_ID FROM EMD.GOODS_PROD WHERE PROD_ID = ?";

	private static final String DISCOUNT_THRESHOLD_SELECT_SQL = ProductDiscountThresholdRowMapper.SELECT_SQL + " WHERE PROD_ID = ?";

	private static final BigDecimalDoubleScaledConverter BIG_DECIMAL_DOUBLE_SCALED_CONVERTER = new BigDecimalDoubleScaledConverter();

	private static final RowMapper<GoodsProduct> MIN_MAX_ORDER_QUANTITY_ROW_MAPPER = (rs, rowNum) ->
		new GoodsProduct()
				.setProdId(rs.getLong("PROD_ID"))
				.setMinimumCustomerOrderQuantity(BIG_DECIMAL_DOUBLE_SCALED_CONVERTER.convertToEntityAttribute(rs.getBigDecimal("MIN_CUST_ORD_QTY")))
				.setMaximumCustomerOrderQuantity(BIG_DECIMAL_DOUBLE_SCALED_CONVERTER.convertToEntityAttribute(rs.getBigDecimal("MAX_CUST_ORD_QTY")));

	private static final SingleRowResultSetExtractor<GoodsProduct> SINGLE_PRODUCT_EXTRACTOR = new SingleRowResultSetExtractor<>(MIN_MAX_ORDER_QUANTITY_ROW_MAPPER);


	private static final ProductDiscountThresholdRowMapper PRODUCT_DISCOUNT_THRESHOLD_ROW_MAPPER = new ProductDiscountThresholdRowMapper();

	private final transient MaintenanceConfig maintenanceConfig;
	private final transient ProductLookup productLookup;

	/**
	 * BatchPreparedStatementSetter for handling updating minimum and maximum order quantities.
	 *
	 * @author d116773
	 * @since 1.24.0
	 */
	private static class MinMaxOrderQuantitySetter extends BaseGoodsProductSetter {

		private MinMaxOrderQuantitySetter(Collection<GoodsProduct> products) {
			super(products);
		}

		@Override
		protected int setProductValues(@Nonnull PreparedStatement ps, @Nonnull GoodsProduct goodsProduct) throws SQLException {

			ps.setBigDecimal(1, goodsProduct.getMinimumCustomerOrderQuantity());
			ps.setBigDecimal(2, goodsProduct.getMaximumCustomerOrderQuantity());
			return 2;
		}
	}

	/**
	 * Constructs a new MinMaxOrderQuantityMaintenance object.
	 *
	 * @param maintenanceConfig The configuration for this class to use.
	 */
	public MinMaxOrderQuantityMaintenance(MaintenanceConfig maintenanceConfig) {

		maintenanceConfig.requireBasics();

		this.maintenanceConfig = maintenanceConfig;
		this.productLookup = new ProductLookup(this.maintenanceConfig.getJdbcTemplate());
	}

	/**
	 * Processes a ProductMaintenanceRequest for a single product. If min or max order quantity is present, it will
	 * update the product accordingly.
	 *
	 * @param productId The ID of the product to do the update on.
	 * @param productMaintenanceRequest The ProductMaintenanceRequest to process.
	 */
	@Transactional
	public void handleMinMaxOrderQuantity(long productId, ProductMaintenanceRequest productMaintenanceRequest) {

		this.maintenanceConfig.requireForProcessing();

		// We only need to do this if they are changing the minimum or maximum order quantity or adding a
		// discount threshold.
		if (Objects.nonNull(productMaintenanceRequest.getMinimumOrderQuantity()) || Objects.nonNull(productMaintenanceRequest.getMaximumOrderQuantity()) ||
				!productMaintenanceRequest.getDiscountThresholds().isEmpty()) {

			DecimalFormat decimalFormat = new DecimalFormat("0.00");

			this.maintenanceConfig.getLogger().info(String.format("Updating min-max order quantity of product %d to {%s, %s} with %d discount thresholds.",
					productId, decimalFormat.format(productMaintenanceRequest.getMinimumOrderQuantity()),
					decimalFormat.format(productMaintenanceRequest.getMaximumOrderQuantity()),
					productMaintenanceRequest.getDiscountThresholds().size()));

			ExtendedProductMaintenanceRequest extendedProductMaintenanceRequest = new ExtendedProductMaintenanceRequest().setProductId(productId);

			extendedProductMaintenanceRequest.setMinimumOrderQuantity(productMaintenanceRequest.getMinimumOrderQuantity())
					.setMaximumOrderQuantity(productMaintenanceRequest.getMaximumOrderQuantity())
					.setUserId(productMaintenanceRequest.getUserId())
					.getDiscountThresholds().addAll(productMaintenanceRequest.getDiscountThresholds());

			this.handleMinMaxOrderQuantity(List.of(extendedProductMaintenanceRequest), extendedProductMaintenanceRequest.getUserId());
		}
	}


	/**
	 * Processes ProductMaintenanceRequests for multiple products. This overloading assumes all requests should be processed.
	 *
	 * @param productMaintenanceRequests The list of requests to process.
	 * @param userId The ID of the user who triggered the request.
	 */
	@Transactional
	public int handleMinMaxOrderQuantity(List<ExtendedProductMaintenanceRequest> productMaintenanceRequests, String userId) {

		notNullOrIllegalArgument(userId, "User ID is required.");

		// If any of the objects are missing values, copy them from the DB. Then validate the request. Then turn
		// them into GoodsProducts and collect them to update.
		List<GoodsProduct> goodsProducts = productMaintenanceRequests.stream()
				.map(this::overlayExistingValues)
				.map(this::doValidate)
				.map(MinMaxOrderQuantityMaintenance::requestToGoodsProduct)
				.collect(Collectors.toList());

		ProductMaintenanceUtils utils = new ProductMaintenanceUtils(this.maintenanceConfig, userId);

		int totalRowsUpdated = utils.doGoodsProductUpdate(goodsProducts,
				Optional::of,
				goodsProductsToUpdate -> {
					MinMaxOrderQuantitySetter minMaxOrderQuantitySetter = new MinMaxOrderQuantitySetter(goodsProductsToUpdate);
					int[] rowsUpdated = this.maintenanceConfig.getJdbcTemplate().batchUpdate(MIN_MAX_UPDATE_SQL, minMaxOrderQuantitySetter);
					this.maintenanceConfig.getLogger().info(String.format("%,d minimum and maximum order quantities updated.", Arrays.stream(rowsUpdated).sum()));
				});

		TableUpdateSets<ProductDiscountThreshold> allDiscountThresholdChanges = new TableUpdateSets<>();

		// Get any changes to discount threshold.
		productMaintenanceRequests.stream()
				.map(MinMaxOrderQuantityMaintenance::getDiscountThresholdChanges)
				.forEach(allDiscountThresholdChanges::addAll);

		totalRowsUpdated += this.handleInsertDiscountThresholds(allDiscountThresholdChanges.getInserts());
		totalRowsUpdated += this.handleUpdateDiscountThresholds(allDiscountThresholdChanges.getUpdates());

		// Generate and issue the PDTH events.
		List<LegacyEvent> legacyEvents = new LinkedList<>();
		allDiscountThresholdChanges.getInserts().stream()
				.map(dth -> LegacyEventGenerator.generatePDTH(dth, this.maintenanceConfig.getProgramName(), userId, LegacyEventFunction.ADD))
				.forEach(legacyEvents::add);
		allDiscountThresholdChanges.getUpdates().stream()
				.map(dth -> LegacyEventGenerator.generatePDTH(dth, this.maintenanceConfig.getProgramName(), userId, LegacyEventFunction.UPDATE))
				.forEach(legacyEvents::add);
		if (!legacyEvents.isEmpty()) {
			this.maintenanceConfig.getLegacyEventProcessor().addAndFlush(legacyEvents);
		}

		return totalRowsUpdated;
	}

	/**
	 * Converts an ExtendedProductMaintenanceRequest to a GoodsProduct.
	 *
	 * @param extendedProductMaintenanceRequest The ExtendedProductMaintenanceRequest to convert.
	 * @return The converted GoodsProduct.
	 */
	private static GoodsProduct requestToGoodsProduct(ExtendedProductMaintenanceRequest extendedProductMaintenanceRequest) {

		return new GoodsProduct().setProdId(extendedProductMaintenanceRequest.getProductId())
				.setMinimumCustomerOrderQuantity(extendedProductMaintenanceRequest.getMinimumOrderQuantity())
				.setMaximumCustomerOrderQuantity(extendedProductMaintenanceRequest.getMaximumOrderQuantity());
	}


	/**
	 * Inserts a set of records into PROD_DISC_THRH.
	 *
	 * @param toInsert The records to insert.
	 * @return The number of records inserted.
	 */
	private int handleInsertDiscountThresholds(Set<ProductDiscountThreshold> toInsert) {

		if (toInsert.isEmpty()) {
			return 0;
		}

		ProductDiscountThresholdUpdater updater = new ProductDiscountThresholdUpdater(toInsert, UpdateType.INSERT);
		int[] rowsInserted = this.maintenanceConfig.getJdbcTemplate().batchUpdate(ProductDiscountThresholdUpdater.INSERT_SQL, updater);
		return Arrays.stream(rowsInserted).sum();
	}

	/**
	 * Updates a set of records in PROD_DISC_THRH.
	 *
	 * @param toUpdate The records to update.
	 * @return The number of records updated.
	 */
	private int handleUpdateDiscountThresholds(Set<ProductDiscountThreshold> toUpdate) {

		if (toUpdate.isEmpty()) {
			return 0;
		}

		ProductDiscountThresholdUpdater updater = new ProductDiscountThresholdUpdater(toUpdate, UpdateType.UPDATE);
		int[] rowsInserted = this.maintenanceConfig.getJdbcTemplate().batchUpdate(ProductDiscountThresholdUpdater.UPDATE_SQL, updater);
		return Arrays.stream(rowsInserted).sum();
	}


	/**
	 * If some of the values are not present in the request, this method will grab them from the DB as they are all needed for validation.
	 *
	 * @param extendedProductMaintenanceRequest The ExtendedProductMaintenanceRequest to overlay database values for.
	 * @return The same ExtendedProductMaintenanceRequest updated with new values.
	 */
	private ExtendedProductMaintenanceRequest overlayExistingValues(ExtendedProductMaintenanceRequest extendedProductMaintenanceRequest) {

		// We only need to do it if they are not all present.
		if (Objects.isNull(extendedProductMaintenanceRequest.getMinimumOrderQuantity()) ||
				Objects.isNull(extendedProductMaintenanceRequest.getMaximumOrderQuantity()) &&
						Objects.nonNull(extendedProductMaintenanceRequest.getProductId())) {

			GoodsProduct goodsProduct = this.maintenanceConfig.getJdbcTemplate().query(MIN_MAX_ORDER_QUANTITY_SELECT_SQL,
					JdbcUtils.argsAsArray(extendedProductMaintenanceRequest.getProductId()),
					SINGLE_PRODUCT_EXTRACTOR);

			if (Objects.nonNull(goodsProduct)) {

				// If extendedProductMaintenanceRequest has nulls, copy the values from goods prod.
				extendedProductMaintenanceRequest.setMinimumOrderQuantity(MappingUtils.valueOrDefault(extendedProductMaintenanceRequest.getMinimumOrderQuantity(),
						goodsProduct.getMinimumCustomerOrderQuantity()));
				extendedProductMaintenanceRequest.setMaximumOrderQuantity(MappingUtils.valueOrDefault(extendedProductMaintenanceRequest.getMaximumOrderQuantity(),
						goodsProduct.getMaximumCustomerOrderQuantity()));
			} else {

				// If it's not there, this will be thrown out during validation anyway. This will copy
				// defaults just to make sure we don't get too many error messages.
				extendedProductMaintenanceRequest.setMinimumOrderQuantity(
						MappingUtils.valueOrDefault(extendedProductMaintenanceRequest.getMinimumOrderQuantity(), MINIMUM_MINIMUM_ORDER_QUANTITY));
				extendedProductMaintenanceRequest.setMaximumOrderQuantity(
						MappingUtils.valueOrDefault(extendedProductMaintenanceRequest.getMaximumOrderQuantity(), MAXIMUM_MAXIMUM_ORDER_QUANTITY));
			}
		}

		// There is old code that set the max to a really large number. If it has that value,
		// set it to the new maximum.
		if (extendedProductMaintenanceRequest.getMaximumOrderQuantity().compareTo(OLD_MAXIMUM_DEFAULT) == 0) {
			extendedProductMaintenanceRequest.setMaximumOrderQuantity(MAXIMUM_MAXIMUM_ORDER_QUANTITY);
		}

		// Get the existing discount thresholds from the DB.
		extendedProductMaintenanceRequest.getExistingDiscountThresholds().clear();
		List<ProductDiscountThreshold> existingDiscountThresholds =
				this.maintenanceConfig.getJdbcTemplate().query(DISCOUNT_THRESHOLD_SELECT_SQL,
						JdbcUtils.argsAsArray(extendedProductMaintenanceRequest.getProductId()),
						PRODUCT_DISCOUNT_THRESHOLD_ROW_MAPPER);
		extendedProductMaintenanceRequest.getExistingDiscountThresholds().addAll(existingDiscountThresholds);

		return extendedProductMaintenanceRequest;
	}

	/**
	 * Takes an object with the existing data from PROD_DSC_THRH and any data the user wants to add and figures out which updates and inserts
	 * need to be made.
	 *
	 * @param extendedProductMaintenanceRequest The object with the existing and new objects.
	 * @return A TableUpdateSets with inserts and updates to process. Each list may be empty.
	 */
	private static TableUpdateSets<ProductDiscountThreshold> getDiscountThresholdChanges(ExtendedProductMaintenanceRequest extendedProductMaintenanceRequest) {

		TableUpdateSets<ProductDiscountThreshold> updates = new TableUpdateSets<>();

		// If there's none in the request, then there's no changes.
		if (extendedProductMaintenanceRequest.getDiscountThresholds().isEmpty()) {
			return updates;
		}

		for (DiscountThreshold discountThreshold : extendedProductMaintenanceRequest.getDiscountThresholds()) {

			ProductDiscountThreshold convertedToDbObject = requestToDiscountThreshold(extendedProductMaintenanceRequest.getProductId(),
					extendedProductMaintenanceRequest.getUserId(), discountThreshold);

			// See if one already exists with the same key.
			Optional<ProductDiscountThreshold> matchingExistingWrapper = extendedProductMaintenanceRequest.getExistingDiscountThresholds().stream()
					.filter(dth -> Objects.equals(convertedToDbObject.getKey(), dth.getKey()))
					.findFirst();

			// If one exists...
			if (matchingExistingWrapper.isPresent()) {

				// Update it.
				updates.getUpdates().add(convertedToDbObject);
			} else {

				// If not, if there are fewer than 5 records, just insert it.
				if (extendedProductMaintenanceRequest.getExistingDiscountThresholds().size() < DESIRED_MAXIMUM_DISCOUNT_THRESHOLDS_PER_PRODUCT) {

					updates.getInserts().add(convertedToDbObject);
				} else {

					// If any match on the same product and discount threshold, add it.
					// TODO: This rule is super weird. I've put a note to see if it is correct.
					boolean matchingOnThresholdQuantityOnly = extendedProductMaintenanceRequest.getExistingDiscountThresholds().stream()
							.anyMatch(pth -> Objects.equals(pth.getKey().getMinimumDiscountThresholdQuantity(),
									convertedToDbObject.getKey().getMinimumDiscountThresholdQuantity()));

					if (matchingOnThresholdQuantityOnly) {
						updates.getInserts().add(convertedToDbObject);
					}
				}

			}
		}

		return updates;
	}

	/**
	 * Converts a DiscountThreshold model object to a ProductDiscountThreshold entity object.
	 *
	 * @param productId The ID of the product this ProductDiscountThreshold is for.
	 * @param userId The ID of the user who triggered the change.
	 * @param discountThreshold The model DiscountThreshold to convert.
	 * @return The converted entity ProductDiscountThreshold.
	 */
	private static ProductDiscountThreshold requestToDiscountThreshold(long productId, String userId, DiscountThreshold discountThreshold) {

		ProductDiscountThresholdKey key = new ProductDiscountThresholdKey()
				.setProductId(productId)
				.setEffectiveTime(InstantUtils.startOfDate(discountThreshold.getEffectiveDate()))
				.setMinimumDiscountThresholdQuantity(discountThreshold.getThresholdQuantity());

		String type = DiscountThreshold.Type.PERCENT_OFF.equals(discountThreshold.getType()) ?
				ProductDiscountThreshold.PERCENT_OFF : ProductDiscountThreshold.CENTS_OFF;

		return new ProductDiscountThreshold().setKey(key)
				.setLastUpdateUserId(userId)
				.setCreateUserId(userId)
				.setLastUpdateTime(Instant.now())
				.setCreateTime(Instant.now())
				.setThresholdDiscountAmount(discountThreshold.getAmount())
				.setThresholdDiscountTypeCode(type);
	}

	/**
	 * Validates a request to change the minimum and maximum order quantities.
	 *
	 * @param productMaintenanceRequest The requests to validate.
	 * @throws ValidationException Any error in validating.
	 */
	public void validate(ExtendedProductMaintenanceRequest productMaintenanceRequest) {

		// Make a copy of this request.
		ExtendedProductMaintenanceRequest copy = new ExtendedProductMaintenanceRequest()
				.setProductId(productMaintenanceRequest.getProductId());

		copy.setMinimumOrderQuantity(productMaintenanceRequest.getMinimumOrderQuantity())
				.setMaximumOrderQuantity(productMaintenanceRequest.getMaximumOrderQuantity())
				.setUserId(productMaintenanceRequest.getUserId())
				.getDiscountThresholds().addAll(productMaintenanceRequest.getDiscountThresholds());

		// Add anything from the DB needed for validation.
		this.overlayExistingValues(copy);

		// Do the validation.
		this.doValidate(copy);
	}

	/**
	 * Internal function that validates a request to change the minimum and maximum order quantities.
	 *
	 * @param productMaintenanceRequest The requests to validate.
	 * @throws ValidationException Any error in validating.
	 */
	protected ExtendedProductMaintenanceRequest doValidate(ExtendedProductMaintenanceRequest productMaintenanceRequest) {

		List<String> errors = new LinkedList<>();

		// Prod ID cannot be null.
		if (Objects.isNull(productMaintenanceRequest.getProductId())) {
			errors.add("Product ID cannot be empty.");
		} else {
			if (!this.productLookup.isGoodsProduct(productMaintenanceRequest.getProductId())) {
				errors.add(String.format("%d is not a valid product ID.", productMaintenanceRequest.getProductId()));
			}
		}

		// User ID is required.
		if (Objects.isNull(productMaintenanceRequest.getUserId())) {
			errors.add("User ID cannot be empty.");
		}

		// Min and max quantity must be there.
		if (Objects.isNull(productMaintenanceRequest.getMinimumOrderQuantity()) || Objects.isNull(productMaintenanceRequest.getMaximumOrderQuantity())) {
			errors.add("Minimum and Maximum order quantity are required.");
		} else {

			DecimalFormat decimalFormat = new DecimalFormat("0.00");

			// Minimum must be greater than the 0.00.
			if (productMaintenanceRequest.getMinimumOrderQuantity().compareTo(MINIMUM_MINIMUM_ORDER_QUANTITY) < 0) {
				errors.add(String.format("Minimum order quantity must be %s or greater.", decimalFormat.format(MINIMUM_MINIMUM_ORDER_QUANTITY)));
			}
			// Maximum must be less than 9999.00
			if (productMaintenanceRequest.getMaximumOrderQuantity().compareTo(MAXIMUM_MAXIMUM_ORDER_QUANTITY) > 0) {
				errors.add(String.format("Maximum order quantity must be %s or fewer.", decimalFormat.format(MAXIMUM_MAXIMUM_ORDER_QUANTITY)));
			}

			// Min must be less than max.
			if (productMaintenanceRequest.getMinimumOrderQuantity().compareTo(productMaintenanceRequest.getMaximumOrderQuantity()) > 0) {
				errors.add("Minimum order quantity must be fewer than or equal to maximum order quantity.");
			}

			long minimumQuantityAsLong = productMaintenanceRequest.getMinimumOrderQuantity().longValue();

			// Check the existing discount thresholds to make sure the minimum quantity fewer than the discount thresholds
			productMaintenanceRequest.getExistingDiscountThresholds().stream()
					.map(pdt -> this.validate(minimumQuantityAsLong, pdt))
					.filter(Optional::isPresent)
					.map(Optional::get)
					.forEach(errors::add);

			// Validate any discount thresholds we're adding.
			productMaintenanceRequest.getDiscountThresholds().stream()
					.map(dt -> this.validate(minimumQuantityAsLong, dt))
					.forEach(errors::addAll);
		}

		if (!errors.isEmpty()) {
			throw new ValidationException("Unable to validate request to change minimum and maximum order quantity.", errors);
		}

		return productMaintenanceRequest;
	}

	private Optional<String> validate(long minimumOrderQuantity, ProductDiscountThreshold productDiscountThreshold) {

		// The new minimum order quantity must be fewer than any existing discount thresholds.
		if (productDiscountThreshold.getKey().getMinimumDiscountThresholdQuantity() < minimumOrderQuantity) {
			return Optional.of(String.format("Minimum discount threshold of %d is fewer than minimum order quantity of %d.",
					productDiscountThreshold.getKey().getMinimumDiscountThresholdQuantity(), minimumOrderQuantity));
		}

		return Optional.empty();
	}

	private List<String> validate(long minimumOrderQuantity, DiscountThreshold discountThreshold) {

		List<String> errors = new LinkedList<>();

		if (Objects.isNull(discountThreshold.getThresholdQuantity())) {

			errors.add("A threshold quantity is required.");
		} else if (discountThreshold.getThresholdQuantity() < minimumOrderQuantity) {

			errors.add(String.format("Minimum discount threshold of %d is fewer than minimum order quantity of %d.",
					discountThreshold.getThresholdQuantity(), minimumOrderQuantity));
		}

		if (Objects.isNull(discountThreshold.getEffectiveDate())) {

			errors.add("Effective date is required for minimum discount threshold.");
		} else if (discountThreshold.getEffectiveDate().isBefore(DateUtils.today())) {

			errors.add("Discount threshold effective date must be today or after.");
		}

		if (Objects.isNull(discountThreshold.getAmount())) {

			errors.add("A discount amount is required.");
		} else if (discountThreshold.getAmount().compareTo(BigDecimal.ZERO) <= 0) {

			errors.add("Discount threshold must be greater than zero.");
		}

		validateFieldExists(discountThreshold::getType, "A discount type is required.").ifPresent(errors::add);

		return errors;
	}
}
