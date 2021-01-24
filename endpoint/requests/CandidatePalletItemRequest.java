package com.heb.pm.core.endpoint.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.heb.pm.core.model.dsdcontainer.CandidatePalletRelatedItem;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Represents a candidate pallet item.
 *
 * @author vn73545
 * @since 2.6.0
 */
@Data
@Accessors(chain = true)
@RequiredArgsConstructor(staticName = "of", onConstructor_ = {@JsonCreator})
public class CandidatePalletItemRequest {

    @ApiModelProperty("The pallet offer id.")
    private Long id;

    @ApiModelProperty("The item code.")
    private Long itemCode;

    @ApiModelProperty("The item key type code.")
    private String itemKeyTypeCode;

    @ApiModelProperty("The item description.")
    private String itemDescription;

    @ApiModelProperty("The bdm code.")
    private String bdmCode;

    @ApiModelProperty("The omi commodity class code.")
    private Long omiCommodityClassCode;

    @ApiModelProperty("The omi commodity code.")
    private Long omiCommodityCode;

    @ApiModelProperty("The omi sub commodity code.")
    private Long omiSubCommodityCode;

    @ApiModelProperty("The department id.")
    private Long departmentId;

    @ApiModelProperty("The sub department id.")
    private String subDepartmentId;

    @ApiModelProperty("The pallet candidate status code.")
    private String palletCandidateStatusCode;

    @ApiModelProperty("The retail pack quantity.")
    private Long retailPackQuantity;

    @ApiModelProperty("The pallet net cost.")
    private BigDecimal palletNetCost;

    @ApiModelProperty("The delivery from date.")
    private Instant deliveryFromDate;

    @ApiModelProperty("The delivery to date.")
    private Instant deliveryToDate;

    @ApiModelProperty("The account payable type code.")
    private String accountPayableTypeCode;

    @ApiModelProperty("The account payable number.")
    private Long accountPayableNumber;

    @ApiModelProperty("The last update user id.")
    private String userId;

    @ApiModelProperty("The candidate pallet related item.")
    private List<CandidatePalletRelatedItem> candidatePalletRelatedItems;

    @ApiModelProperty("The candidate pallet related item.")
    private List<CandidatePalletRelatedItem> deleteCandidatePalletRelatedItems;
}
