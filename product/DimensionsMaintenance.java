package com.heb.pm.core.service.maintenance.product;

import com.heb.pm.core.exception.ValidationException;
import com.heb.pm.core.maintenance.ProductMaintenanceRequest;
import com.heb.pm.core.model.ProductDimensions;
import com.heb.pm.core.service.maintenance.common.MaintenanceConfig;
import com.heb.pm.core.service.maintenance.common.TableUpdateSets;
import com.heb.pm.dao.core.DatabaseConstants;
import com.heb.pm.dao.core.entity.GoodsProduct;
import com.heb.pm.dao.core.entity.ProductShippingHandling;
import com.heb.pm.dao.core.entity.ProductShippingHandlingKey;
import com.heb.pm.dao.core.entity.codes.ConsumerPurchaseChoice;
import com.heb.pm.dao.core.entity.codes.ProductShippingHandlingType;
import com.heb.pm.dao.core.preparedstatementsetters.ProductShippingHandlingUpdater;
import com.heb.pm.dao.core.preparedstatementsetters.UpdateType;
import com.heb.pm.dao.core.quicklookup.ProductLookup;
import com.heb.pm.util.BigDecimalUtils;
import com.heb.pm.util.MappingUtils;
import com.heb.pm.util.ValidatorUtils;
import com.heb.pm.util.jpa.SwitchToBooleanConverter;
import lombok.Getter;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Manages maintaining the dimension attributes on GOODS_PROD.
 *
 * @author d116773
 * @since 1.25.0
 */
public class DimensionsMaintenance {

	private static final BigDecimal MINIMUM_WEIGHT_FOR_LARGE = new BigDecimal("69.0000");
	private static final BigDecimal MINIMUM_WEIGHT_FOR_EXTRA_LARGE = new BigDecimal("88.0000");

	private static final BigDecimal MINIMUM_LENGTH_FOR_LARGE = new BigDecimal("59.50");
	private static final BigDecimal MINIUM_MIDDLE_LENGTH_FOR_LARGE = new BigDecimal("29.50");
	private static final BigDecimal MINIMUM_LENGTH_FOR_EXTRA_LARGE = new BigDecimal("129.00");

	private static final String DIMENSIONS_UPDATE_SQL = "UPDATE EMD.GOODS_PROD " +
			"SET RETL_UNT_LN = ?, RETL_UNT_WD = ?, RETL_UNT_HT = ?, RETL_UNT_WT = ?, CONSM_PRCH_CHC_CD = ?, WT_SW = ?, LRGE_PROD_SHPN_SW = ?, " +
			"	LST_UPDT_UID = ?, LST_UPDT_TS = ?, LST_SYS_UPDT_ID = ? " +
			"WHERE PROD_ID = ?";

	private final transient MaintenanceConfig config;
	private final transient ProductLookup productLookup;
	private final transient DimensionsLookup dimensionsLookup;

	/**
	 * Wraps a GoodsProduct and ProductShippingHandlingType that need to be updated together.
	 *
	 * @author d116773
	 * @since 1.25.0
	 */
	@Getter
	private static final class GoodsProductAndShippingHandlingType {

		private final transient GoodsProduct goodsProduct;
		private final transient ProductShippingHandlingType productShippingHandlingType;

		private GoodsProductAndShippingHandlingType(GoodsProduct goodsProduct, ProductShippingHandlingType productShippingHandlingType) {
			this.goodsProduct = goodsProduct;
			this.productShippingHandlingType = productShippingHandlingType;
		}
	}

	/**
	 * PreparedStatementSetter fo the dimension fields in GOODS_PRO.
	 *
	 * @author d116773
	 * @since 1.25.0
	 */
	private static class DimensionsSetter extends BaseGoodsProductSetter {

		private static final SwitchToBooleanConverter BOOLEAN_CONVERTER = new SwitchToBooleanConverter();

		/**
		 * Constructs a new DimensionsSetter.
		 *
		 * @param products The GoodsProducts to update.
		 */
		private DimensionsSetter(Collection<GoodsProduct> products) {
			super(products);
		}

		@Override
		protected int setProductValues(@Nonnull PreparedStatement ps, @Nonnull GoodsProduct goodsProduct) throws SQLException {

			ps.setBigDecimal(1, goodsProduct.getRetailUnitLength());
			ps.setBigDecimal(2, goodsProduct.getRetailUnitWidth());
			ps.setBigDecimal(3, goodsProduct.getRetailUnitHeight());
			ps.setBigDecimal(4, goodsProduct.getRetailUnitWeight());
			ps.setString(5, goodsProduct.getSoldBy());
			ps.setString(6, goodsProduct.getSoldByWeightSwitch());
			ps.setString(7, BOOLEAN_CONVERTER.convertToDatabaseColumn(goodsProduct.getLargeProductShippingSwitch()));
			return 7;
		}
	}

	/**
	 * Constructs a new DimensionsMaintenance.
	 *
	 * @param config The configuration for this class to use.
	 */
	public DimensionsMaintenance(MaintenanceConfig config) {
		this.config = config;
		this.config.requireBasics();
		this.productLookup = new ProductLookup(this.config.getJdbcTemplate());
		this.dimensionsLookup = new DimensionsLookup(this.config.getJdbcTemplate());
	}

	/**
	 * If the ProductMaintenanceRequest contains a request to change the dimension data, processes that request.
	 *
	 * @param productId The product to update.
	 * @param productMaintenanceRequest The ProductMaintenanceRequest to process.
	 * @throws ValidationException Any error validating the request.
	 */
	@Transactional
	public void handleDimensionRequest(long productId, ProductMaintenanceRequest productMaintenanceRequest) {

		if (Objects.nonNull(productMaintenanceRequest.getDimensions())) {

			this.config.getLogger().info("Updating dimesions of %d to %s.", productId, productMaintenanceRequest.getDimensions());

			ExtendedProductMaintenanceRequest extendedProductMaintenanceRequest = new ExtendedProductMaintenanceRequest().setProductId(productId);
			extendedProductMaintenanceRequest.setDimensions(productMaintenanceRequest.getDimensions());
			this.handleDimensionRequests(List.of(extendedProductMaintenanceRequest), productMaintenanceRequest.getUserId());
		}

	}

	/**
	 * Processes a list of ExtendedProductMaintenanceRequests.
	 *
	 * @param requests The list of requests to process.
	 * @param userId The user who triggered the request.
	 * @return The number of records modified.
	 * @throws ValidationException Any error validating the request.
	 */
	@Transactional
	public int handleDimensionRequests(List<ExtendedProductMaintenanceRequest> requests, String userId) {

		this.config.requireForProcessing();

		// Validate the requests, convert them into goods prods, and collect them to be saved.
		List<GoodsProductAndShippingHandlingType> updates = requests.stream()
				.map(this::overlayExisting)
				.map(this::doValidate)
				.map(DimensionsMaintenance::requestToGoodsProduct)
				.collect(Collectors.toList());

		List<GoodsProduct> goodsProducts = updates.stream()
				.map(GoodsProductAndShippingHandlingType::getGoodsProduct)
				.collect(Collectors.toList());

		TableUpdateSets<ProductShippingHandling> shippingHandlingUpdates = new TableUpdateSets<>();

		updates.stream()
				.map(p -> this.getShippingHandlingChanges(p, userId))
				.forEach(shippingHandlingUpdates::addAll);

		ProductMaintenanceUtils utils = new ProductMaintenanceUtils(this.config, userId);

		int totalRowsUpdated = utils.doGoodsProductUpdate(goodsProducts,
				Optional::of,
				goodsProductsToUpdate -> {
					DimensionsSetter dimensionsSetter = new DimensionsSetter(goodsProductsToUpdate);
					int[] rowsUpdated = this.config.getJdbcTemplate().batchUpdate(DIMENSIONS_UPDATE_SQL, dimensionsSetter);
					this.config.getLogger().info(String.format("%,d product dimensions updated.", Arrays.stream(rowsUpdated).sum()));
				});

		totalRowsUpdated += this.doShippingHandlingDeletes(shippingHandlingUpdates.getDeletes());
		totalRowsUpdated += this.doShippingHandlingInserts(shippingHandlingUpdates.getInserts());

		return totalRowsUpdated;
	}

	/**
	 * Validates a ExtendedProductMaintenanceRequest for a change in product dimensions.
	 *
	 * @param request The ExtendedProductMaintenanceRequest to validate.
	 * @throws IllegalArgumentException If no dimensional data is present.
	 * @throws ValidationException Any error validating the request.
	 */
	public void validate(ExtendedProductMaintenanceRequest request) {

		if (Objects.isNull(request.getDimensions())) {

			throw new IllegalArgumentException("Dimensions are required for this validation.");
		}

		// Make a copy of the request and validate that.
		ExtendedProductMaintenanceRequest copiedRequest = new ExtendedProductMaintenanceRequest().setProductId(request.getProductId());

		ProductDimensions productDimensions = request.getDimensions();
		ProductDimensions copiedDimensions = ProductDimensions.of().setFlexWeight(productDimensions.getFlexWeight())
				.setHeight(productDimensions.getHeight())
				.setLength(productDimensions.getLength())
				.setWidth(productDimensions.getWidth())
				.setWeight(productDimensions.getWeight())
				.setType(productDimensions.getType());
		copiedRequest.setDimensions(copiedDimensions);

		this.doValidate(this.overlayExisting(copiedRequest));
	}

	/**
	 * Calculates if a product is LARGE or EXTRA-LARGE for the purposes of shipping. It will return the appropriate
	 * ProductShippingHandlingType based on the calculation if it is LARGE or EXTRA-LARGE and empty if it is neither.
	 *
	 * @param productDimensions The ProductDimensions update request.
	 * @return The appropriate ProductShippingHandlingType or empty.
	 */
	protected static Optional<ProductShippingHandlingType> calculateProductSize(ProductDimensions productDimensions) {

		// I don't believe these calculations are correct. I've reached out to the business for confirmations.

		// Sort the dimensions and get the shortest, middlest, and longest sides.
		List<BigDecimal> allSides = Arrays.asList(productDimensions.getLength(), productDimensions.getWidth(), productDimensions.getHeight());
		allSides.sort(BigDecimal::compareTo);

		BigDecimal shortestSide = allSides.get(0);
		BigDecimal middleSide = allSides.get(1);
		BigDecimal longestSide = allSides.get(2);

		// First, see if it is extra large.
		// >= 88 pounds
		// or
		// the longest side + (2 * shortest side) + (2 * middlest side) >= 129
		if (BigDecimalUtils.isLessThanOrEqualTo(MINIMUM_WEIGHT_FOR_EXTRA_LARGE, productDimensions.getWeight()) ||
				BigDecimalUtils.isLessThanOrEqualTo(MINIMUM_LENGTH_FOR_EXTRA_LARGE,
						extraLargeSizeCalculation(longestSide, middleSide, shortestSide))) {

			return Optional.of(ProductShippingHandlingType.EXTRA_LARGE);
		}

		// If it's not extra large, see if it is large.
		// >= 69 pounds
		// or
		// the shortest side >= 59.5
		// or
		// the middle side >= 29.5
		if (BigDecimalUtils.isLessThanOrEqualTo(MINIMUM_WEIGHT_FOR_LARGE, productDimensions.getWeight()) ||
				BigDecimalUtils.isLessThanOrEqualTo(MINIMUM_LENGTH_FOR_LARGE, shortestSide) ||
				BigDecimalUtils.isLessThanOrEqualTo(MINIUM_MIDDLE_LENGTH_FOR_LARGE, middleSide)) {

			return Optional.of(ProductShippingHandlingType.LARGE);
		}

		// If none of these, it is not large or extra large, so return empty.
		return Optional.empty();
	}

	/**
	 * Does the calculation to see if something is extra-large based on it's size.
	 *
	 * @param longest The length of the longest side of the product.
	 * @param middle The length of the middle side of the product.
	 * @param smallest The length of the shortest side of the product.
	 * @return The value to compare against MINIMUM_LENGTH_FOR_EXTRA_LARGE to see if this product is extra-large.
	 */
	private static BigDecimal extraLargeSizeCalculation(BigDecimal longest, BigDecimal middle, BigDecimal smallest) {

		BigDecimal twoTimesMiddle = middle.multiply(new BigDecimal("2.00"));
		BigDecimal twoTimesSmallest = smallest.multiply(new BigDecimal("2.00"));

		return longest.add(twoTimesMiddle).add(twoTimesSmallest);
	}

	/**
	 * Figures out what changes need to be made to the PROD_SHPNG_HNDLG based on the current and new states of a product.
	 *
	 * @param request The GoodsProductAndShippingHandlingType that contains the GOODS_PRODUCT and PROD_SHPNG_HNDLG data to save to the tables.
	 * @param userId The ID of the user who triggered this change.
	 * @return A TableUpdateSets possibly containing an insert to and delete from PROD_SHPNG_HNDLG.
	 */
	private TableUpdateSets<ProductShippingHandling> getShippingHandlingChanges(GoodsProductAndShippingHandlingType request, String userId) {

		TableUpdateSets<ProductShippingHandling> updates = new TableUpdateSets<>();

		// See if there's an existing ProductShippingHandling record.
		Optional<ProductShippingHandling> existingRecord = this.dimensionsLookup.findShippingHandlingByProduct(request.getGoodsProduct().getProdId());

		ProductShippingHandlingType existingType = existingRecord.map(psh -> psh.getKey().getProductShippingHandlingCode()).orElse(null);

		// If they're the same, no changes.
		if (Objects.equals(existingType, request.getProductShippingHandlingType())) {
			return updates;
		}

		// We need to delete the old and add the new.
		existingRecord.ifPresent(updates.getDeletes()::add);

		if (Objects.nonNull(request.getProductShippingHandlingType())) {

			ProductShippingHandlingKey key = new ProductShippingHandlingKey().setProductId(request.getGoodsProduct().getProdId())
					.setProductShippingHandlingCode(request.getProductShippingHandlingType());

			updates.getInserts().add(new ProductShippingHandling().setKey(key).setCreateId(userId));
		}

		return updates;
	}

	/**
	 * Deletes records from PROD_SHPNG_HNDLG.
	 *
	 * @param toDelete The records to delete.
	 * @return The number of rows deleted.
	 */
	private int doShippingHandlingDeletes(Set<ProductShippingHandling> toDelete) {

		if (toDelete.isEmpty()) {
			return 0;
		}

		ProductShippingHandlingUpdater updater = new ProductShippingHandlingUpdater(toDelete, UpdateType.DELETE);
		int[] rowsUpdated = this.config.getJdbcTemplate().batchUpdate(ProductShippingHandlingUpdater.DELETE_SQL, updater);
		return Arrays.stream(rowsUpdated).sum();
	}

	/**
	 * Inserts records into PROD_SHPNG_HNDLG.
	 *
	 * @param toInsert The records to insert.
	 * @return The number of rows inserted.
	 */
	private int doShippingHandlingInserts(Set<ProductShippingHandling> toInsert) {

		if (toInsert.isEmpty()) {
			return 0;
		}

		ProductShippingHandlingUpdater updater = new ProductShippingHandlingUpdater(toInsert, UpdateType.INSERT);
		int[] rowsUpdated = this.config.getJdbcTemplate().batchUpdate(ProductShippingHandlingUpdater.INSERT_SQL, updater);
		return Arrays.stream(rowsUpdated).sum();
	}

	/**
	 * Takes a ExtendedProductMaintenanceRequest, looks up the product's current data, and overlays any null fields
	 * related to updating dimensions and populates it with the existing data. If the product is not found, it will
	 * do nothing.
	 *
	 * @param request The ExtendedProductMaintenanceRequest to process.
	 * @return The updated ExtendedProductMaintenanceRequest.
	 */
	private ExtendedProductMaintenanceRequest overlayExisting(ExtendedProductMaintenanceRequest request) {

		ProductDimensions productDimensions = request.getDimensions();

		// We only need to pull the data if anything is blank.
		if (Objects.nonNull(request.getProductId()) &&
				(Objects.isNull(productDimensions.getFlexWeight()) || Objects.isNull(productDimensions.getHeight()) ||
				Objects.isNull(productDimensions.getLength()) || Objects.isNull(productDimensions.getWidth()) ||
				Objects.isNull(productDimensions.getType()) || Objects.isNull(productDimensions.getWeight()))) {

			// Lookup the product and overlay what is already there. If it's not a valid product, the validator
			// will handle it.

			this.dimensionsLookup.findById(request.getProductId()).ifPresent(existingRecord -> {

				productDimensions.setLength(MappingUtils.valueOrDefault(productDimensions.getLength(), existingRecord.getRetailUnitLength()));
				productDimensions.setWidth(MappingUtils.valueOrDefault(productDimensions.getWidth(), existingRecord.getRetailUnitWidth()));
				productDimensions.setHeight(MappingUtils.valueOrDefault(productDimensions.getHeight(), existingRecord.getRetailUnitHeight()));
				productDimensions.setWeight(MappingUtils.valueOrDefault(productDimensions.getWeight(), existingRecord.getRetailUnitWeight()));
				productDimensions.setFlexWeight(MappingUtils.valueOrDefault(productDimensions.getFlexWeight(),
						Objects.equals(existingRecord.getSoldByWeightSwitch(), DatabaseConstants.YES)));
				productDimensions.setType(MappingUtils.valueOrDefault(productDimensions.getType(), codeToType(existingRecord.getEcommerceSoldBy())));
			});
		}

		return request;
	}

	/**
	 * Converts an ExtendedProductMaintenanceRequest to a GoodsProductAndShippingHandlingType. The GoodsProduct will be
	 * populated but the ProductShippingHandlingType may be null.
	 *
	 * @param request The ExtendedProductMaintenanceRequest to convert.
	 * @return The converted GoodsProductAndShippingHandlingType.
	 */
	private static GoodsProductAndShippingHandlingType requestToGoodsProduct(ExtendedProductMaintenanceRequest request) {

		ProductDimensions productDimensions = request.getDimensions();

		GoodsProduct goodsProduct = new GoodsProduct().setProdId(request.getProductId())
				.setRetailUnitLength(productDimensions.getLength())
				.setRetailUnitWidth(productDimensions.getWidth())
				.setRetailUnitHeight(productDimensions.getHeight())
				.setRetailUnitWeight(productDimensions.getWeight())
				.setSoldBy(typeToCode(productDimensions.getType()).getId())
				.setSoldByWeightSwitch(productDimensions.getFlexWeight() ? DatabaseConstants.YES : DatabaseConstants.NO);

		Optional<ProductShippingHandlingType> largeOrExtraLarge = calculateProductSize(request.getDimensions());

		goodsProduct.setLargeProductShippingSwitch(largeOrExtraLarge.isPresent());

		return new GoodsProductAndShippingHandlingType(goodsProduct, largeOrExtraLarge.orElse(null));
	}

	/**
	 * Validates a ExtendedProductMaintenanceRequest containing a dimension update request.
	 *
	 * @param request The ExtendedProductMaintenanceRequest to validate.
	 * @return The validated ExtendedProductMaintenanceRequest.
	 * @throws ValidationException Any error validating the request.
	 */
	private ExtendedProductMaintenanceRequest doValidate(ExtendedProductMaintenanceRequest request) {

		List<String> errors = new LinkedList<>();

		if (Objects.isNull(request.getProductId())) {

			errors.add("Product ID is required.");
		} else if (!this.productLookup.isGoodsProduct(request.getProductId())) {

			errors.add(String.format("%d is not a valid product.", request.getProductId()));
		}

		// We don't have a valid product, don't check other things.
		if (errors.isEmpty()) {

			ProductDimensions productDimensions = request.getDimensions();

			ValidatorUtils.validateFieldExists(productDimensions::getLength, "Length is required.").ifPresent(errors::add);
			ValidatorUtils.validateFieldExists(productDimensions::getWidth, "Width is required.").ifPresent(errors::add);
			ValidatorUtils.validateFieldExists(productDimensions::getHeight, "Height is required.").ifPresent(errors::add);
			ValidatorUtils.validateFieldExists(productDimensions::getWeight, "Weight is required.").ifPresent(errors::add);
			ValidatorUtils.validateFieldExists(productDimensions::getFlexWeight, "Flex weight is required.").ifPresent(errors::add);
			ValidatorUtils.validateFieldExists(productDimensions::getType, "Merchandise type is required.").ifPresent(errors::add);

			// If it's weight or each weight, a retail weight is required.
			if (!Objects.equals(productDimensions.getType(), ProductDimensions.MerchandiseType.EACH) &&
					BigDecimalUtils.isLessThanOrEqualTo(productDimensions.getWeight(), BigDecimal.ZERO)) {

				errors.add("When a product is sold by weight, a retail weight is required.");
			}
		}

		if (!errors.isEmpty()) {
			throw new ValidationException("Unable to validate dimension request.", errors);
		}

		return request;
	}

	/**
	 * Convets a ConsumerPurchaseChoice to a ProductDimensions.MerchandiseType.
	 *
	 * @param consumerPurchaseChoice The ConsumerPurchaseChoice to convert.
	 * @return The converted ProductDimensions.MerchandiseType.
	 */
	private static ProductDimensions.MerchandiseType codeToType(ConsumerPurchaseChoice consumerPurchaseChoice) {

		switch (consumerPurchaseChoice) {
			case EACH:
				return ProductDimensions.MerchandiseType.EACH;
			case WEIGHT:
				return ProductDimensions.MerchandiseType.WEIGHT;
			case EACH_WEIGHT:
				return ProductDimensions.MerchandiseType.EACH_WEIGHT;
			default:
				throw new IllegalArgumentException(String.format("Unknown type %s.", consumerPurchaseChoice));
		}
	}

	/**
	 * Converts a ProductDimensions.MerchandiseType to a ConsumerPurchaseChoice.
	 *
	 * @param merchandiseType The ProductDimensions.MerchandiseType to convert.
	 * @return The converted ConsumerPurchaseChoice.
	 */
	private static ConsumerPurchaseChoice typeToCode(ProductDimensions.MerchandiseType merchandiseType) {

		switch (merchandiseType) {
			case EACH:
				return ConsumerPurchaseChoice.EACH;
			case WEIGHT:
				return ConsumerPurchaseChoice.WEIGHT;
			case EACH_WEIGHT:
				return ConsumerPurchaseChoice.EACH_WEIGHT;
			default:
				throw new IllegalArgumentException(String.format("Unknown type %s.", merchandiseType));
		}
	}
}
