#![allow(dead_code)]

#[derive(Clone, Copy, Debug)]
pub(super) struct Col {
    pub(super) number: usize,
    pub(super) per_row: u32,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Span {
    pub(super) first: usize,
    pub(super) last: usize,
}

pub(super) const MAX_COLUMN: usize = 799;

pub(super) const PER_ROW: [u32; MAX_COLUMN + 1] = [
    0, 0, 15, 10, 1, 4, 10, 0, 2, 1, 1, 1, 2, 1, 50, 50, 2, 2, 2, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 15, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 100, 1, 2, 28, 28, 14, 14, 14, 14, 14, 28, 14, 14, 14,
    0, 14, 14, 14, 14, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 28, 70, 1, 2, 14, 1, 1, 1, 1, 2, 1, 1, 1,
    1, 42, 14, 14, 1, 14, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 112, 0, 28, 14, 0, 1,
    1, 1, 1, 1, 0, 1, 1, 1, 1, 2, 1, 0, 0, 1, 1, 23, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    2, 7, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 1, 1, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 1, 2, 1, 1, 1, 2, 200, 2,
    1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 50, 1, 1, 1, 2, 1, 2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 100, 1, 1, 2, 1, 1, 1, 2, 1, 1, 1, 1, 21, 1, 2, 1, 1, 1, 2, 2, 0, 1, 1, 1, 2, 1, 1,
    1, 100, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 7, 32, 32, 16, 16, 16, 16, 16,
    16, 16, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 80, 32, 2, 2, 1, 1, 1, 1, 1, 1, 16, 1, 1, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 16, 128, 32, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 80, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 2, 7, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 32, 32, 16, 16, 16, 16,
    16, 16, 16, 16, 16, 16, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 80, 32, 2, 2, 16, 1, 1, 1, 1, 1,
    2, 2, 1, 1, 16, 16, 16, 16, 16, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 32, 16,
    0, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 20, 100, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 7, 70,
    0, 0, 0, 0, 0, 0, 0, 1, 1, 6, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 7, 1, 1, 1, 0, 0, 0, 1, 1, 1, 1,
    1, 1, 1, 1, 0, 1, 1, 10, 1, 0, 1, 2, 1, 1, 1, 1, 1, 100, 1, 0, 1, 1, 1, 1, 1, 1, 2, 8, 0, 1, 0,
    0, 0, 1, 1, 1, 1, 100, 1, 1, 1, 1, 0, 9, 0, 0, 0, 0, 0, 9, 18, 0, 0, 0, 0, 0, 0, 0, 0, 9, 72,
    9, 9, 0, 0, 0, 10, 0, 1, 10, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 25, 23, 1, 1, 4, 4, 7, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 1, 1, 2, 1, 1, 0, 1, 200, 2,
    1, 1, 1, 50, 1, 1, 1, 1, 0, 2, 0, 0, 0, 0, 10, 1, 0, 1, 0, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0,
    0, 0, 0, 100, 1, 1, 1, 0, 1, 0, 1, 1, 1, 1, 1, 1, 100, 7, 1, 12, 12, 12, 12, 12, 12, 1200, 96,
    12, 0, 0, 10, 1, 1, 1, 1, 1, 0, 0, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 24, 12, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 12, 84, 0, 1, 0, 10, 0, 1, 0, 10, 1, 10, 1, 1, 1, 1, 2, 1, 1, 1, 1,
    100, 1, 12, 0, 12, 12, 12, 12, 12, 12, 96, 0, 12, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1,
    1, 1, 1, 0, 0, 0, 0, 0, 0, 24, 12, 12, 84, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 0,
];

pub(super) const CC_CALL_CENTER_SK: Col = Col {
    number: 1,
    per_row: 0,
};
pub(super) const CC_CALL_CENTER_ID: Col = Col {
    number: 2,
    per_row: 15,
};
pub(super) const CC_REC_START_DATE_ID: Col = Col {
    number: 3,
    per_row: 10,
};
pub(super) const CC_REC_END_DATE_ID: Col = Col {
    number: 4,
    per_row: 1,
};
pub(super) const CC_CLOSED_DATE_ID: Col = Col {
    number: 5,
    per_row: 4,
};
pub(super) const CC_OPEN_DATE_ID: Col = Col {
    number: 6,
    per_row: 10,
};
pub(super) const CC_NAME: Col = Col {
    number: 7,
    per_row: 0,
};
pub(super) const CC_CLASS: Col = Col {
    number: 8,
    per_row: 2,
};
pub(super) const CC_EMPLOYEES: Col = Col {
    number: 9,
    per_row: 1,
};
pub(super) const CC_SQ_FT: Col = Col {
    number: 10,
    per_row: 1,
};
pub(super) const CC_HOURS: Col = Col {
    number: 11,
    per_row: 1,
};
pub(super) const CC_MANAGER: Col = Col {
    number: 12,
    per_row: 2,
};
pub(super) const CC_MARKET_ID: Col = Col {
    number: 13,
    per_row: 1,
};
pub(super) const CC_MARKET_CLASS: Col = Col {
    number: 14,
    per_row: 50,
};
pub(super) const CC_MARKET_DESC: Col = Col {
    number: 15,
    per_row: 50,
};
pub(super) const CC_MARKET_MANAGER: Col = Col {
    number: 16,
    per_row: 2,
};
pub(super) const CC_DIVISION: Col = Col {
    number: 17,
    per_row: 2,
};
pub(super) const CC_DIVISION_NAME: Col = Col {
    number: 18,
    per_row: 2,
};
pub(super) const CC_COMPANY: Col = Col {
    number: 19,
    per_row: 2,
};
pub(super) const CC_COMPANY_NAME: Col = Col {
    number: 20,
    per_row: 2,
};
pub(super) const CC_STREET_NUMBER: Col = Col {
    number: 21,
    per_row: 0,
};
pub(super) const CC_STREET_NAME: Col = Col {
    number: 22,
    per_row: 0,
};
pub(super) const CC_STREET_TYPE: Col = Col {
    number: 23,
    per_row: 0,
};
pub(super) const CC_SUITE_NUMBER: Col = Col {
    number: 24,
    per_row: 0,
};
pub(super) const CC_CITY: Col = Col {
    number: 25,
    per_row: 0,
};
pub(super) const CC_COUNTY: Col = Col {
    number: 26,
    per_row: 0,
};
pub(super) const CC_STATE: Col = Col {
    number: 27,
    per_row: 0,
};
pub(super) const CC_ZIP: Col = Col {
    number: 28,
    per_row: 0,
};
pub(super) const CC_COUNTRY: Col = Col {
    number: 29,
    per_row: 0,
};
pub(super) const CC_GMT_OFFSET: Col = Col {
    number: 30,
    per_row: 0,
};
pub(super) const CC_ADDRESS: Col = Col {
    number: 31,
    per_row: 15,
};
pub(super) const CC_TAX_PERCENTAGE: Col = Col {
    number: 32,
    per_row: 1,
};
pub(super) const CC_SCD: Col = Col {
    number: 33,
    per_row: 1,
};
pub(super) const CC_NULLS: Col = Col {
    number: 34,
    per_row: 2,
};
pub(super) const CP_CATALOG_PAGE_SK: Col = Col {
    number: 35,
    per_row: 1,
};
pub(super) const CP_CATALOG_PAGE_ID: Col = Col {
    number: 36,
    per_row: 1,
};
pub(super) const CP_START_DATE_ID: Col = Col {
    number: 37,
    per_row: 1,
};
pub(super) const CP_END_DATE_ID: Col = Col {
    number: 38,
    per_row: 1,
};
pub(super) const CP_PROMO_ID: Col = Col {
    number: 39,
    per_row: 1,
};
pub(super) const CP_DEPARTMENT: Col = Col {
    number: 40,
    per_row: 1,
};
pub(super) const CP_CATALOG_NUMBER: Col = Col {
    number: 41,
    per_row: 1,
};
pub(super) const CP_CATALOG_PAGE_NUMBER: Col = Col {
    number: 42,
    per_row: 1,
};
pub(super) const CP_DESCRIPTION: Col = Col {
    number: 43,
    per_row: 100,
};
pub(super) const CP_TYPE: Col = Col {
    number: 44,
    per_row: 1,
};
pub(super) const CP_NULLS: Col = Col {
    number: 45,
    per_row: 2,
};
pub(super) const CR_RETURNED_DATE_SK: Col = Col {
    number: 46,
    per_row: 28,
};
pub(super) const CR_RETURNED_TIME_SK: Col = Col {
    number: 47,
    per_row: 28,
};
pub(super) const CR_ITEM_SK: Col = Col {
    number: 48,
    per_row: 14,
};
pub(super) const CR_REFUNDED_CUSTOMER_SK: Col = Col {
    number: 49,
    per_row: 14,
};
pub(super) const CR_REFUNDED_CDEMO_SK: Col = Col {
    number: 50,
    per_row: 14,
};
pub(super) const CR_REFUNDED_HDEMO_SK: Col = Col {
    number: 51,
    per_row: 14,
};
pub(super) const CR_REFUNDED_ADDR_SK: Col = Col {
    number: 52,
    per_row: 14,
};
pub(super) const CR_RETURNING_CUSTOMER_SK: Col = Col {
    number: 53,
    per_row: 28,
};
pub(super) const CR_RETURNING_CDEMO_SK: Col = Col {
    number: 54,
    per_row: 14,
};
pub(super) const CR_RETURNING_HDEMO_SK: Col = Col {
    number: 55,
    per_row: 14,
};
pub(super) const CR_RETURNING_ADDR_SK: Col = Col {
    number: 56,
    per_row: 14,
};
pub(super) const CR_CALL_CENTER_SK: Col = Col {
    number: 57,
    per_row: 0,
};
pub(super) const CR_CATALOG_PAGE_SK: Col = Col {
    number: 58,
    per_row: 14,
};
pub(super) const CR_SHIP_MODE_SK: Col = Col {
    number: 59,
    per_row: 14,
};
pub(super) const CR_WAREHOUSE_SK: Col = Col {
    number: 60,
    per_row: 14,
};
pub(super) const CR_REASON_SK: Col = Col {
    number: 61,
    per_row: 14,
};
pub(super) const CR_ORDER_NUMBER: Col = Col {
    number: 62,
    per_row: 0,
};
pub(super) const CR_PRICING_QUANTITY: Col = Col {
    number: 63,
    per_row: 0,
};
pub(super) const CR_PRICING_NET_PAID: Col = Col {
    number: 64,
    per_row: 0,
};
pub(super) const CR_PRICING_EXT_TAX: Col = Col {
    number: 65,
    per_row: 0,
};
pub(super) const CR_PRICING_NET_PAID_INC_TAX: Col = Col {
    number: 66,
    per_row: 0,
};
pub(super) const CR_PRICING_FEE: Col = Col {
    number: 67,
    per_row: 0,
};
pub(super) const CR_PRICING_EXT_SHIP_COST: Col = Col {
    number: 68,
    per_row: 0,
};
pub(super) const CR_PRICING_REFUNDED_CASH: Col = Col {
    number: 69,
    per_row: 0,
};
pub(super) const CR_PRICING_REVERSED_CHARGE: Col = Col {
    number: 70,
    per_row: 0,
};
pub(super) const CR_PRICING_STORE_CREDIT: Col = Col {
    number: 71,
    per_row: 0,
};
pub(super) const CR_PRICING_NET_LOSS: Col = Col {
    number: 72,
    per_row: 0,
};
pub(super) const CR_NULLS: Col = Col {
    number: 73,
    per_row: 28,
};
pub(super) const CR_PRICING: Col = Col {
    number: 74,
    per_row: 70,
};
pub(super) const CS_SOLD_DATE_SK: Col = Col {
    number: 75,
    per_row: 1,
};
pub(super) const CS_SOLD_TIME_SK: Col = Col {
    number: 76,
    per_row: 2,
};
pub(super) const CS_SHIP_DATE_SK: Col = Col {
    number: 77,
    per_row: 14,
};
pub(super) const CS_BILL_CUSTOMER_SK: Col = Col {
    number: 78,
    per_row: 1,
};
pub(super) const CS_BILL_CDEMO_SK: Col = Col {
    number: 79,
    per_row: 1,
};
pub(super) const CS_BILL_HDEMO_SK: Col = Col {
    number: 80,
    per_row: 1,
};
pub(super) const CS_BILL_ADDR_SK: Col = Col {
    number: 81,
    per_row: 1,
};
pub(super) const CS_SHIP_CUSTOMER_SK: Col = Col {
    number: 82,
    per_row: 2,
};
pub(super) const CS_SHIP_CDEMO_SK: Col = Col {
    number: 83,
    per_row: 1,
};
pub(super) const CS_SHIP_HDEMO_SK: Col = Col {
    number: 84,
    per_row: 1,
};
pub(super) const CS_SHIP_ADDR_SK: Col = Col {
    number: 85,
    per_row: 1,
};
pub(super) const CS_CALL_CENTER_SK: Col = Col {
    number: 86,
    per_row: 1,
};
pub(super) const CS_CATALOG_PAGE_SK: Col = Col {
    number: 87,
    per_row: 42,
};
pub(super) const CS_SHIP_MODE_SK: Col = Col {
    number: 88,
    per_row: 14,
};
pub(super) const CS_WAREHOUSE_SK: Col = Col {
    number: 89,
    per_row: 14,
};
pub(super) const CS_SOLD_ITEM_SK: Col = Col {
    number: 90,
    per_row: 1,
};
pub(super) const CS_PROMO_SK: Col = Col {
    number: 91,
    per_row: 14,
};
pub(super) const CS_ORDER_NUMBER: Col = Col {
    number: 92,
    per_row: 1,
};
pub(super) const CS_PRICING_QUANTITY: Col = Col {
    number: 93,
    per_row: 0,
};
pub(super) const CS_PRICING_WHOLESALE_COST: Col = Col {
    number: 94,
    per_row: 0,
};
pub(super) const CS_PRICING_LIST_PRICE: Col = Col {
    number: 95,
    per_row: 0,
};
pub(super) const CS_PRICING_SALES_PRICE: Col = Col {
    number: 96,
    per_row: 0,
};
pub(super) const CS_PRICING_COUPON_AMT: Col = Col {
    number: 97,
    per_row: 0,
};
pub(super) const CS_PRICING_EXT_SALES_PRICE: Col = Col {
    number: 98,
    per_row: 0,
};
pub(super) const CS_PRICING_EXT_DISCOUNT_AMOUNT: Col = Col {
    number: 99,
    per_row: 0,
};
pub(super) const CS_PRICING_EXT_WHOLESALE_COST: Col = Col {
    number: 100,
    per_row: 0,
};
pub(super) const CS_PRICING_EXT_LIST_PRICE: Col = Col {
    number: 101,
    per_row: 0,
};
pub(super) const CS_PRICING_EXT_TAX: Col = Col {
    number: 102,
    per_row: 0,
};
pub(super) const CS_PRICING_EXT_SHIP_COST: Col = Col {
    number: 103,
    per_row: 0,
};
pub(super) const CS_PRICING_NET_PAID: Col = Col {
    number: 104,
    per_row: 0,
};
pub(super) const CS_PRICING_NET_PAID_INC_TAX: Col = Col {
    number: 105,
    per_row: 0,
};
pub(super) const CS_PRICING_NET_PAID_INC_SHIP: Col = Col {
    number: 106,
    per_row: 0,
};
pub(super) const CS_PRICING_NET_PAID_INC_SHIP_TAX: Col = Col {
    number: 107,
    per_row: 0,
};
pub(super) const CS_PRICING_NET_PROFIT: Col = Col {
    number: 108,
    per_row: 0,
};
pub(super) const CS_PRICING: Col = Col {
    number: 109,
    per_row: 112,
};
pub(super) const CS_PERMUTE: Col = Col {
    number: 110,
    per_row: 0,
};
pub(super) const CS_NULLS: Col = Col {
    number: 111,
    per_row: 28,
};
pub(super) const CR_IS_RETURNED: Col = Col {
    number: 112,
    per_row: 14,
};
pub(super) const CS_PERMUTATION: Col = Col {
    number: 113,
    per_row: 0,
};
pub(super) const C_CUSTOMER_SK: Col = Col {
    number: 114,
    per_row: 1,
};
pub(super) const C_CUSTOMER_ID: Col = Col {
    number: 115,
    per_row: 1,
};
pub(super) const C_CURRENT_CDEMO_SK: Col = Col {
    number: 116,
    per_row: 1,
};
pub(super) const C_CURRENT_HDEMO_SK: Col = Col {
    number: 117,
    per_row: 1,
};
pub(super) const C_CURRENT_ADDR_SK: Col = Col {
    number: 118,
    per_row: 1,
};
pub(super) const C_FIRST_SHIPTO_DATE_ID: Col = Col {
    number: 119,
    per_row: 0,
};
pub(super) const C_FIRST_SALES_DATE_ID: Col = Col {
    number: 120,
    per_row: 1,
};
pub(super) const C_SALUTATION: Col = Col {
    number: 121,
    per_row: 1,
};
pub(super) const C_FIRST_NAME: Col = Col {
    number: 122,
    per_row: 1,
};
pub(super) const C_LAST_NAME: Col = Col {
    number: 123,
    per_row: 1,
};
pub(super) const C_PREFERRED_CUST_FLAG: Col = Col {
    number: 124,
    per_row: 2,
};
pub(super) const C_BIRTH_DAY: Col = Col {
    number: 125,
    per_row: 1,
};
pub(super) const C_BIRTH_MONTH: Col = Col {
    number: 126,
    per_row: 0,
};
pub(super) const C_BIRTH_YEAR: Col = Col {
    number: 127,
    per_row: 0,
};
pub(super) const C_BIRTH_COUNTRY: Col = Col {
    number: 128,
    per_row: 1,
};
pub(super) const C_LOGIN: Col = Col {
    number: 129,
    per_row: 1,
};
pub(super) const C_EMAIL_ADDRESS: Col = Col {
    number: 130,
    per_row: 23,
};
pub(super) const C_LAST_REVIEW_DATE: Col = Col {
    number: 131,
    per_row: 1,
};
pub(super) const C_NULLS: Col = Col {
    number: 132,
    per_row: 2,
};
pub(super) const CA_ADDRESS_SK: Col = Col {
    number: 133,
    per_row: 1,
};
pub(super) const CA_ADDRESS_ID: Col = Col {
    number: 134,
    per_row: 1,
};
pub(super) const CA_ADDRESS_STREET_NUM: Col = Col {
    number: 135,
    per_row: 1,
};
pub(super) const CA_ADDRESS_STREET_NAME1: Col = Col {
    number: 136,
    per_row: 1,
};
pub(super) const CA_ADDRESS_STREET_TYPE: Col = Col {
    number: 137,
    per_row: 1,
};
pub(super) const CA_ADDRESS_SUITE_NUM: Col = Col {
    number: 138,
    per_row: 1,
};
pub(super) const CA_ADDRESS_CITY: Col = Col {
    number: 139,
    per_row: 1,
};
pub(super) const CA_ADDRESS_COUNTY: Col = Col {
    number: 140,
    per_row: 1,
};
pub(super) const CA_ADDRESS_STATE: Col = Col {
    number: 141,
    per_row: 1,
};
pub(super) const CA_ADDRESS_ZIP: Col = Col {
    number: 142,
    per_row: 1,
};
pub(super) const CA_ADDRESS_COUNTRY: Col = Col {
    number: 143,
    per_row: 1,
};
pub(super) const CA_ADDRESS_GMT_OFFSET: Col = Col {
    number: 144,
    per_row: 1,
};
pub(super) const CA_LOCATION_TYPE: Col = Col {
    number: 145,
    per_row: 1,
};
pub(super) const CA_NULLS: Col = Col {
    number: 146,
    per_row: 2,
};
pub(super) const CA_ADDRESS: Col = Col {
    number: 147,
    per_row: 7,
};
pub(super) const CA_ADDRESS_STREET_NAME2: Col = Col {
    number: 148,
    per_row: 1,
};
pub(super) const CD_DEMO_SK: Col = Col {
    number: 149,
    per_row: 1,
};
pub(super) const CD_GENDER: Col = Col {
    number: 150,
    per_row: 1,
};
pub(super) const CD_MARITAL_STATUS: Col = Col {
    number: 151,
    per_row: 1,
};
pub(super) const CD_EDUCATION_STATUS: Col = Col {
    number: 152,
    per_row: 1,
};
pub(super) const CD_PURCHASE_ESTIMATE: Col = Col {
    number: 153,
    per_row: 1,
};
pub(super) const CD_CREDIT_RATING: Col = Col {
    number: 154,
    per_row: 1,
};
pub(super) const CD_DEP_COUNT: Col = Col {
    number: 155,
    per_row: 1,
};
pub(super) const CD_DEP_EMPLOYED_COUNT: Col = Col {
    number: 156,
    per_row: 1,
};
pub(super) const CD_DEP_COLLEGE_COUNT: Col = Col {
    number: 157,
    per_row: 1,
};
pub(super) const CD_NULLS: Col = Col {
    number: 158,
    per_row: 2,
};
pub(super) const D_DATE_SK: Col = Col {
    number: 159,
    per_row: 0,
};
pub(super) const D_DATE_ID: Col = Col {
    number: 160,
    per_row: 0,
};
pub(super) const D_DATE: Col = Col {
    number: 161,
    per_row: 0,
};
pub(super) const D_MONTH_SEQ: Col = Col {
    number: 162,
    per_row: 0,
};
pub(super) const D_WEEK_SEQ: Col = Col {
    number: 163,
    per_row: 0,
};
pub(super) const D_QUARTER_SEQ: Col = Col {
    number: 164,
    per_row: 0,
};
pub(super) const D_YEAR: Col = Col {
    number: 165,
    per_row: 0,
};
pub(super) const D_DOW: Col = Col {
    number: 166,
    per_row: 0,
};
pub(super) const D_MOY: Col = Col {
    number: 167,
    per_row: 0,
};
pub(super) const D_DOM: Col = Col {
    number: 168,
    per_row: 0,
};
pub(super) const D_QOY: Col = Col {
    number: 169,
    per_row: 0,
};
pub(super) const D_FY_YEAR: Col = Col {
    number: 170,
    per_row: 0,
};
pub(super) const D_FY_QUARTER_SEQ: Col = Col {
    number: 171,
    per_row: 0,
};
pub(super) const D_FY_WEEK_SEQ: Col = Col {
    number: 172,
    per_row: 0,
};
pub(super) const D_DAY_NAME: Col = Col {
    number: 173,
    per_row: 0,
};
pub(super) const D_QUARTER_NAME: Col = Col {
    number: 174,
    per_row: 0,
};
pub(super) const D_HOLIDAY: Col = Col {
    number: 175,
    per_row: 0,
};
pub(super) const D_WEEKEND: Col = Col {
    number: 176,
    per_row: 0,
};
pub(super) const D_FOLLOWING_HOLIDAY: Col = Col {
    number: 177,
    per_row: 0,
};
pub(super) const D_FIRST_DOM: Col = Col {
    number: 178,
    per_row: 0,
};
pub(super) const D_LAST_DOM: Col = Col {
    number: 179,
    per_row: 0,
};
pub(super) const D_SAME_DAY_LY: Col = Col {
    number: 180,
    per_row: 0,
};
pub(super) const D_SAME_DAY_LQ: Col = Col {
    number: 181,
    per_row: 0,
};
pub(super) const D_CURRENT_DAY: Col = Col {
    number: 182,
    per_row: 0,
};
pub(super) const D_CURRENT_WEEK: Col = Col {
    number: 183,
    per_row: 0,
};
pub(super) const D_CURRENT_MONTH: Col = Col {
    number: 184,
    per_row: 0,
};
pub(super) const D_CURRENT_QUARTER: Col = Col {
    number: 185,
    per_row: 0,
};
pub(super) const D_CURRENT_YEAR: Col = Col {
    number: 186,
    per_row: 0,
};
pub(super) const D_NULLS: Col = Col {
    number: 187,
    per_row: 2,
};
pub(super) const HD_DEMO_SK: Col = Col {
    number: 188,
    per_row: 1,
};
pub(super) const HD_INCOME_BAND_ID: Col = Col {
    number: 189,
    per_row: 1,
};
pub(super) const HD_BUY_POTENTIAL: Col = Col {
    number: 190,
    per_row: 1,
};
pub(super) const HD_DEP_COUNT: Col = Col {
    number: 191,
    per_row: 1,
};
pub(super) const HD_VEHICLE_COUNT: Col = Col {
    number: 192,
    per_row: 1,
};
pub(super) const HD_NULLS: Col = Col {
    number: 193,
    per_row: 2,
};
pub(super) const IB_INCOME_BAND_ID: Col = Col {
    number: 194,
    per_row: 1,
};
pub(super) const IB_LOWER_BOUND: Col = Col {
    number: 195,
    per_row: 1,
};
pub(super) const IB_UPPER_BOUND: Col = Col {
    number: 196,
    per_row: 1,
};
pub(super) const IB_NULLS: Col = Col {
    number: 197,
    per_row: 2,
};
pub(super) const INV_DATE_SK: Col = Col {
    number: 198,
    per_row: 1,
};
pub(super) const INV_ITEM_SK: Col = Col {
    number: 199,
    per_row: 1,
};
pub(super) const INV_WAREHOUSE_SK: Col = Col {
    number: 200,
    per_row: 1,
};
pub(super) const INV_QUANTITY_ON_HAND: Col = Col {
    number: 201,
    per_row: 1,
};
pub(super) const INV_NULLS: Col = Col {
    number: 202,
    per_row: 2,
};
pub(super) const I_ITEM_SK: Col = Col {
    number: 203,
    per_row: 1,
};
pub(super) const I_ITEM_ID: Col = Col {
    number: 204,
    per_row: 1,
};
pub(super) const I_REC_START_DATE_ID: Col = Col {
    number: 205,
    per_row: 1,
};
pub(super) const I_REC_END_DATE_ID: Col = Col {
    number: 206,
    per_row: 2,
};
pub(super) const I_ITEM_DESC: Col = Col {
    number: 207,
    per_row: 200,
};
pub(super) const I_CURRENT_PRICE: Col = Col {
    number: 208,
    per_row: 2,
};
pub(super) const I_WHOLESALE_COST: Col = Col {
    number: 209,
    per_row: 1,
};
pub(super) const I_BRAND_ID: Col = Col {
    number: 210,
    per_row: 1,
};
pub(super) const I_BRAND: Col = Col {
    number: 211,
    per_row: 1,
};
pub(super) const I_CLASS_ID: Col = Col {
    number: 212,
    per_row: 1,
};
pub(super) const I_CLASS: Col = Col {
    number: 213,
    per_row: 1,
};
pub(super) const I_CATEGORY_ID: Col = Col {
    number: 214,
    per_row: 1,
};
pub(super) const I_CATEGORY: Col = Col {
    number: 215,
    per_row: 1,
};
pub(super) const I_MANUFACT_ID: Col = Col {
    number: 216,
    per_row: 2,
};
pub(super) const I_MANUFACT: Col = Col {
    number: 217,
    per_row: 1,
};
pub(super) const I_SIZE: Col = Col {
    number: 218,
    per_row: 1,
};
pub(super) const I_FORMULATION: Col = Col {
    number: 219,
    per_row: 50,
};
pub(super) const I_COLOR: Col = Col {
    number: 220,
    per_row: 1,
};
pub(super) const I_UNITS: Col = Col {
    number: 221,
    per_row: 1,
};
pub(super) const I_CONTAINER: Col = Col {
    number: 222,
    per_row: 1,
};
pub(super) const I_MANAGER_ID: Col = Col {
    number: 223,
    per_row: 2,
};
pub(super) const I_PRODUCT_NAME: Col = Col {
    number: 224,
    per_row: 1,
};
pub(super) const I_NULLS: Col = Col {
    number: 225,
    per_row: 2,
};
pub(super) const I_SCD: Col = Col {
    number: 226,
    per_row: 1,
};
pub(super) const I_PROMO_SK: Col = Col {
    number: 227,
    per_row: 2,
};
pub(super) const P_PROMO_SK: Col = Col {
    number: 228,
    per_row: 1,
};
pub(super) const P_PROMO_ID: Col = Col {
    number: 229,
    per_row: 1,
};
pub(super) const P_START_DATE_ID: Col = Col {
    number: 230,
    per_row: 1,
};
pub(super) const P_END_DATE_ID: Col = Col {
    number: 231,
    per_row: 1,
};
pub(super) const P_ITEM_SK: Col = Col {
    number: 232,
    per_row: 1,
};
pub(super) const P_COST: Col = Col {
    number: 233,
    per_row: 1,
};
pub(super) const P_RESPONSE_TARGET: Col = Col {
    number: 234,
    per_row: 1,
};
pub(super) const P_PROMO_NAME: Col = Col {
    number: 235,
    per_row: 1,
};
pub(super) const P_CHANNEL_DMAIL: Col = Col {
    number: 236,
    per_row: 1,
};
pub(super) const P_CHANNEL_EMAIL: Col = Col {
    number: 237,
    per_row: 1,
};
pub(super) const P_CHANNEL_CATALOG: Col = Col {
    number: 238,
    per_row: 1,
};
pub(super) const P_CHANNEL_TV: Col = Col {
    number: 239,
    per_row: 1,
};
pub(super) const P_CHANNEL_RADIO: Col = Col {
    number: 240,
    per_row: 1,
};
pub(super) const P_CHANNEL_PRESS: Col = Col {
    number: 241,
    per_row: 1,
};
pub(super) const P_CHANNEL_EVENT: Col = Col {
    number: 242,
    per_row: 1,
};
pub(super) const P_CHANNEL_DEMO: Col = Col {
    number: 243,
    per_row: 1,
};
pub(super) const P_CHANNEL_DETAILS: Col = Col {
    number: 244,
    per_row: 100,
};
pub(super) const P_PURPOSE: Col = Col {
    number: 245,
    per_row: 1,
};
pub(super) const P_DISCOUNT_ACTIVE: Col = Col {
    number: 246,
    per_row: 1,
};
pub(super) const P_NULLS: Col = Col {
    number: 247,
    per_row: 2,
};
pub(super) const R_REASON_SK: Col = Col {
    number: 248,
    per_row: 1,
};
pub(super) const R_REASON_ID: Col = Col {
    number: 249,
    per_row: 1,
};
pub(super) const R_REASON_DESCRIPTION: Col = Col {
    number: 250,
    per_row: 1,
};
pub(super) const R_NULLS: Col = Col {
    number: 251,
    per_row: 2,
};
pub(super) const SM_SHIP_MODE_SK: Col = Col {
    number: 252,
    per_row: 1,
};
pub(super) const SM_SHIP_MODE_ID: Col = Col {
    number: 253,
    per_row: 1,
};
pub(super) const SM_TYPE: Col = Col {
    number: 254,
    per_row: 1,
};
pub(super) const SM_CODE: Col = Col {
    number: 255,
    per_row: 1,
};
pub(super) const SM_CONTRACT: Col = Col {
    number: 256,
    per_row: 21,
};
pub(super) const SM_CARRIER: Col = Col {
    number: 257,
    per_row: 1,
};
pub(super) const SM_NULLS: Col = Col {
    number: 258,
    per_row: 2,
};
pub(super) const W_STORE_SK: Col = Col {
    number: 259,
    per_row: 1,
};
pub(super) const W_STORE_ID: Col = Col {
    number: 260,
    per_row: 1,
};
pub(super) const W_STORE_REC_START_DATE_ID: Col = Col {
    number: 261,
    per_row: 1,
};
pub(super) const W_STORE_REC_END_DATE_ID: Col = Col {
    number: 262,
    per_row: 2,
};
pub(super) const W_STORE_CLOSED_DATE_ID: Col = Col {
    number: 263,
    per_row: 2,
};
pub(super) const W_STORE_NAME: Col = Col {
    number: 264,
    per_row: 0,
};
pub(super) const W_STORE_EMPLOYEES: Col = Col {
    number: 265,
    per_row: 1,
};
pub(super) const W_STORE_FLOOR_SPACE: Col = Col {
    number: 266,
    per_row: 1,
};
pub(super) const W_STORE_HOURS: Col = Col {
    number: 267,
    per_row: 1,
};
pub(super) const W_STORE_MANAGER: Col = Col {
    number: 268,
    per_row: 2,
};
pub(super) const W_STORE_MARKET_ID: Col = Col {
    number: 269,
    per_row: 1,
};
pub(super) const W_STORE_TAX_PERCENTAGE: Col = Col {
    number: 270,
    per_row: 1,
};
pub(super) const W_STORE_GEOGRAPHY_CLASS: Col = Col {
    number: 271,
    per_row: 1,
};
pub(super) const W_STORE_MARKET_DESC: Col = Col {
    number: 272,
    per_row: 100,
};
pub(super) const W_STORE_MARKET_MANAGER: Col = Col {
    number: 273,
    per_row: 2,
};
pub(super) const W_STORE_DIVISION_ID: Col = Col {
    number: 274,
    per_row: 1,
};
pub(super) const W_STORE_DIVISION_NAME: Col = Col {
    number: 275,
    per_row: 1,
};
pub(super) const W_STORE_COMPANY_ID: Col = Col {
    number: 276,
    per_row: 1,
};
pub(super) const W_STORE_COMPANY_NAME: Col = Col {
    number: 277,
    per_row: 1,
};
pub(super) const W_STORE_ADDRESS_STREET_NUM: Col = Col {
    number: 278,
    per_row: 1,
};
pub(super) const W_STORE_ADDRESS_STREET_NAME1: Col = Col {
    number: 279,
    per_row: 1,
};
pub(super) const W_STORE_ADDRESS_STREET_TYPE: Col = Col {
    number: 280,
    per_row: 1,
};
pub(super) const W_STORE_ADDRESS_SUITE_NUM: Col = Col {
    number: 281,
    per_row: 1,
};
pub(super) const W_STORE_ADDRESS_CITY: Col = Col {
    number: 282,
    per_row: 1,
};
pub(super) const W_STORE_ADDRESS_COUNTY: Col = Col {
    number: 283,
    per_row: 1,
};
pub(super) const W_STORE_ADDRESS_STATE: Col = Col {
    number: 284,
    per_row: 1,
};
pub(super) const W_STORE_ADDRESS_ZIP: Col = Col {
    number: 285,
    per_row: 1,
};
pub(super) const W_STORE_ADDRESS_COUNTRY: Col = Col {
    number: 286,
    per_row: 1,
};
pub(super) const W_STORE_ADDRESS_GMT_OFFSET: Col = Col {
    number: 287,
    per_row: 1,
};
pub(super) const W_STORE_NULLS: Col = Col {
    number: 288,
    per_row: 2,
};
pub(super) const W_STORE_TYPE: Col = Col {
    number: 289,
    per_row: 1,
};
pub(super) const W_STORE_SCD: Col = Col {
    number: 290,
    per_row: 1,
};
pub(super) const W_STORE_ADDRESS: Col = Col {
    number: 291,
    per_row: 7,
};
pub(super) const SR_RETURNED_DATE_SK: Col = Col {
    number: 292,
    per_row: 32,
};
pub(super) const SR_RETURNED_TIME_SK: Col = Col {
    number: 293,
    per_row: 32,
};
pub(super) const SR_ITEM_SK: Col = Col {
    number: 294,
    per_row: 16,
};
pub(super) const SR_CUSTOMER_SK: Col = Col {
    number: 295,
    per_row: 16,
};
pub(super) const SR_CDEMO_SK: Col = Col {
    number: 296,
    per_row: 16,
};
pub(super) const SR_HDEMO_SK: Col = Col {
    number: 297,
    per_row: 16,
};
pub(super) const SR_ADDR_SK: Col = Col {
    number: 298,
    per_row: 16,
};
pub(super) const SR_STORE_SK: Col = Col {
    number: 299,
    per_row: 16,
};
pub(super) const SR_REASON_SK: Col = Col {
    number: 300,
    per_row: 16,
};
pub(super) const SR_TICKET_NUMBER: Col = Col {
    number: 301,
    per_row: 16,
};
pub(super) const SR_PRICING_QUANTITY: Col = Col {
    number: 302,
    per_row: 0,
};
pub(super) const SR_PRICING_NET_PAID: Col = Col {
    number: 303,
    per_row: 0,
};
pub(super) const SR_PRICING_EXT_TAX: Col = Col {
    number: 304,
    per_row: 0,
};
pub(super) const SR_PRICING_NET_PAID_INC_TAX: Col = Col {
    number: 305,
    per_row: 0,
};
pub(super) const SR_PRICING_FEE: Col = Col {
    number: 306,
    per_row: 0,
};
pub(super) const SR_PRICING_EXT_SHIP_COST: Col = Col {
    number: 307,
    per_row: 0,
};
pub(super) const SR_PRICING_REFUNDED_CASH: Col = Col {
    number: 308,
    per_row: 0,
};
pub(super) const SR_PRICING_REVERSED_CHARGE: Col = Col {
    number: 309,
    per_row: 0,
};
pub(super) const SR_PRICING_STORE_CREDIT: Col = Col {
    number: 310,
    per_row: 0,
};
pub(super) const SR_PRICING_NET_LOSS: Col = Col {
    number: 311,
    per_row: 0,
};
pub(super) const SR_PRICING: Col = Col {
    number: 312,
    per_row: 80,
};
pub(super) const SR_NULLS: Col = Col {
    number: 313,
    per_row: 32,
};
pub(super) const SS_SOLD_DATE_SK: Col = Col {
    number: 314,
    per_row: 2,
};
pub(super) const SS_SOLD_TIME_SK: Col = Col {
    number: 315,
    per_row: 2,
};
pub(super) const SS_SOLD_ITEM_SK: Col = Col {
    number: 316,
    per_row: 1,
};
pub(super) const SS_SOLD_CUSTOMER_SK: Col = Col {
    number: 317,
    per_row: 1,
};
pub(super) const SS_SOLD_CDEMO_SK: Col = Col {
    number: 318,
    per_row: 1,
};
pub(super) const SS_SOLD_HDEMO_SK: Col = Col {
    number: 319,
    per_row: 1,
};
pub(super) const SS_SOLD_ADDR_SK: Col = Col {
    number: 320,
    per_row: 1,
};
pub(super) const SS_SOLD_STORE_SK: Col = Col {
    number: 321,
    per_row: 1,
};
pub(super) const SS_SOLD_PROMO_SK: Col = Col {
    number: 322,
    per_row: 16,
};
pub(super) const SS_TICKET_NUMBER: Col = Col {
    number: 323,
    per_row: 1,
};
pub(super) const SS_PRICING_QUANTITY: Col = Col {
    number: 324,
    per_row: 1,
};
pub(super) const SS_PRICING_WHOLESALE_COST: Col = Col {
    number: 325,
    per_row: 0,
};
pub(super) const SS_PRICING_LIST_PRICE: Col = Col {
    number: 326,
    per_row: 0,
};
pub(super) const SS_PRICING_SALES_PRICE: Col = Col {
    number: 327,
    per_row: 0,
};
pub(super) const SS_PRICING_COUPON_AMT: Col = Col {
    number: 328,
    per_row: 0,
};
pub(super) const SS_PRICING_EXT_SALES_PRICE: Col = Col {
    number: 329,
    per_row: 0,
};
pub(super) const SS_PRICING_EXT_WHOLESALE_COST: Col = Col {
    number: 330,
    per_row: 0,
};
pub(super) const SS_PRICING_EXT_LIST_PRICE: Col = Col {
    number: 331,
    per_row: 0,
};
pub(super) const SS_PRICING_EXT_TAX: Col = Col {
    number: 332,
    per_row: 0,
};
pub(super) const SS_PRICING_NET_PAID: Col = Col {
    number: 333,
    per_row: 0,
};
pub(super) const SS_PRICING_NET_PAID_INC_TAX: Col = Col {
    number: 334,
    per_row: 0,
};
pub(super) const SS_PRICING_NET_PROFIT: Col = Col {
    number: 335,
    per_row: 0,
};
pub(super) const SR_IS_RETURNED: Col = Col {
    number: 336,
    per_row: 16,
};
pub(super) const SS_PRICING: Col = Col {
    number: 337,
    per_row: 128,
};
pub(super) const SS_NULLS: Col = Col {
    number: 338,
    per_row: 32,
};
pub(super) const SS_PERMUTATION: Col = Col {
    number: 339,
    per_row: 0,
};
pub(super) const T_TIME_SK: Col = Col {
    number: 340,
    per_row: 1,
};
pub(super) const T_TIME_ID: Col = Col {
    number: 341,
    per_row: 1,
};
pub(super) const T_TIME: Col = Col {
    number: 342,
    per_row: 1,
};
pub(super) const T_HOUR: Col = Col {
    number: 343,
    per_row: 1,
};
pub(super) const T_MINUTE: Col = Col {
    number: 344,
    per_row: 1,
};
pub(super) const T_SECOND: Col = Col {
    number: 345,
    per_row: 1,
};
pub(super) const T_AM_PM: Col = Col {
    number: 346,
    per_row: 1,
};
pub(super) const T_SHIFT: Col = Col {
    number: 347,
    per_row: 1,
};
pub(super) const T_SUB_SHIFT: Col = Col {
    number: 348,
    per_row: 1,
};
pub(super) const T_MEAL_TIME: Col = Col {
    number: 349,
    per_row: 1,
};
pub(super) const T_NULLS: Col = Col {
    number: 350,
    per_row: 2,
};
pub(super) const W_WAREHOUSE_SK: Col = Col {
    number: 351,
    per_row: 1,
};
pub(super) const W_WAREHOUSE_ID: Col = Col {
    number: 352,
    per_row: 1,
};
pub(super) const W_WAREHOUSE_NAME: Col = Col {
    number: 353,
    per_row: 80,
};
pub(super) const W_WAREHOUSE_SQ_FT: Col = Col {
    number: 354,
    per_row: 1,
};
pub(super) const W_ADDRESS_STREET_NUM: Col = Col {
    number: 355,
    per_row: 1,
};
pub(super) const W_ADDRESS_STREET_NAME1: Col = Col {
    number: 356,
    per_row: 1,
};
pub(super) const W_ADDRESS_STREET_TYPE: Col = Col {
    number: 357,
    per_row: 1,
};
pub(super) const W_ADDRESS_SUITE_NUM: Col = Col {
    number: 358,
    per_row: 1,
};
pub(super) const W_ADDRESS_CITY: Col = Col {
    number: 359,
    per_row: 1,
};
pub(super) const W_ADDRESS_COUNTY: Col = Col {
    number: 360,
    per_row: 1,
};
pub(super) const W_ADDRESS_STATE: Col = Col {
    number: 361,
    per_row: 1,
};
pub(super) const W_ADDRESS_ZIP: Col = Col {
    number: 362,
    per_row: 1,
};
pub(super) const W_ADDRESS_COUNTRY: Col = Col {
    number: 363,
    per_row: 1,
};
pub(super) const W_ADDRESS_GMT_OFFSET: Col = Col {
    number: 364,
    per_row: 1,
};
pub(super) const W_NULLS: Col = Col {
    number: 365,
    per_row: 2,
};
pub(super) const W_WAREHOUSE_ADDRESS: Col = Col {
    number: 366,
    per_row: 7,
};
pub(super) const WP_PAGE_SK: Col = Col {
    number: 367,
    per_row: 1,
};
pub(super) const WP_PAGE_ID: Col = Col {
    number: 368,
    per_row: 1,
};
pub(super) const WP_REC_START_DATE_ID: Col = Col {
    number: 369,
    per_row: 1,
};
pub(super) const WP_REC_END_DATE_ID: Col = Col {
    number: 370,
    per_row: 1,
};
pub(super) const WP_CREATION_DATE_SK: Col = Col {
    number: 371,
    per_row: 2,
};
pub(super) const WP_ACCESS_DATE_SK: Col = Col {
    number: 372,
    per_row: 1,
};
pub(super) const WP_AUTOGEN_FLAG: Col = Col {
    number: 373,
    per_row: 1,
};
pub(super) const WP_CUSTOMER_SK: Col = Col {
    number: 374,
    per_row: 1,
};
pub(super) const WP_URL: Col = Col {
    number: 375,
    per_row: 1,
};
pub(super) const WP_TYPE: Col = Col {
    number: 376,
    per_row: 1,
};
pub(super) const WP_CHAR_COUNT: Col = Col {
    number: 377,
    per_row: 1,
};
pub(super) const WP_LINK_COUNT: Col = Col {
    number: 378,
    per_row: 1,
};
pub(super) const WP_IMAGE_COUNT: Col = Col {
    number: 379,
    per_row: 1,
};
pub(super) const WP_MAX_AD_COUNT: Col = Col {
    number: 380,
    per_row: 1,
};
pub(super) const WP_NULLS: Col = Col {
    number: 381,
    per_row: 2,
};
pub(super) const WP_SCD: Col = Col {
    number: 382,
    per_row: 1,
};
pub(super) const WR_RETURNED_DATE_SK: Col = Col {
    number: 383,
    per_row: 32,
};
pub(super) const WR_RETURNED_TIME_SK: Col = Col {
    number: 384,
    per_row: 32,
};
pub(super) const WR_ITEM_SK: Col = Col {
    number: 385,
    per_row: 16,
};
pub(super) const WR_REFUNDED_CUSTOMER_SK: Col = Col {
    number: 386,
    per_row: 16,
};
pub(super) const WR_REFUNDED_CDEMO_SK: Col = Col {
    number: 387,
    per_row: 16,
};
pub(super) const WR_REFUNDED_HDEMO_SK: Col = Col {
    number: 388,
    per_row: 16,
};
pub(super) const WR_REFUNDED_ADDR_SK: Col = Col {
    number: 389,
    per_row: 16,
};
pub(super) const WR_RETURNING_CUSTOMER_SK: Col = Col {
    number: 390,
    per_row: 16,
};
pub(super) const WR_RETURNING_CDEMO_SK: Col = Col {
    number: 391,
    per_row: 16,
};
pub(super) const WR_RETURNING_HDEMO_SK: Col = Col {
    number: 392,
    per_row: 16,
};
pub(super) const WR_RETURNING_ADDR_SK: Col = Col {
    number: 393,
    per_row: 16,
};
pub(super) const WR_WEB_PAGE_SK: Col = Col {
    number: 394,
    per_row: 16,
};
pub(super) const WR_REASON_SK: Col = Col {
    number: 395,
    per_row: 16,
};
pub(super) const WR_ORDER_NUMBER: Col = Col {
    number: 396,
    per_row: 0,
};
pub(super) const WR_PRICING_QUANTITY: Col = Col {
    number: 397,
    per_row: 0,
};
pub(super) const WR_PRICING_NET_PAID: Col = Col {
    number: 398,
    per_row: 0,
};
pub(super) const WR_PRICING_EXT_TAX: Col = Col {
    number: 399,
    per_row: 0,
};
pub(super) const WR_PRICING_NET_PAID_INC_TAX: Col = Col {
    number: 400,
    per_row: 0,
};
pub(super) const WR_PRICING_FEE: Col = Col {
    number: 401,
    per_row: 0,
};
pub(super) const WR_PRICING_EXT_SHIP_COST: Col = Col {
    number: 402,
    per_row: 0,
};
pub(super) const WR_PRICING_REFUNDED_CASH: Col = Col {
    number: 403,
    per_row: 0,
};
pub(super) const WR_PRICING_REVERSED_CHARGE: Col = Col {
    number: 404,
    per_row: 0,
};
pub(super) const WR_PRICING_STORE_CREDIT: Col = Col {
    number: 405,
    per_row: 0,
};
pub(super) const WR_PRICING_NET_LOSS: Col = Col {
    number: 406,
    per_row: 0,
};
pub(super) const WR_PRICING: Col = Col {
    number: 407,
    per_row: 80,
};
pub(super) const WR_NULLS: Col = Col {
    number: 408,
    per_row: 32,
};
pub(super) const WS_SOLD_DATE_SK: Col = Col {
    number: 409,
    per_row: 2,
};
pub(super) const WS_SOLD_TIME_SK: Col = Col {
    number: 410,
    per_row: 2,
};
pub(super) const WS_SHIP_DATE_SK: Col = Col {
    number: 411,
    per_row: 16,
};
pub(super) const WS_ITEM_SK: Col = Col {
    number: 412,
    per_row: 1,
};
pub(super) const WS_BILL_CUSTOMER_SK: Col = Col {
    number: 413,
    per_row: 1,
};
pub(super) const WS_BILL_CDEMO_SK: Col = Col {
    number: 414,
    per_row: 1,
};
pub(super) const WS_BILL_HDEMO_SK: Col = Col {
    number: 415,
    per_row: 1,
};
pub(super) const WS_BILL_ADDR_SK: Col = Col {
    number: 416,
    per_row: 1,
};
pub(super) const WS_SHIP_CUSTOMER_SK: Col = Col {
    number: 417,
    per_row: 2,
};
pub(super) const WS_SHIP_CDEMO_SK: Col = Col {
    number: 418,
    per_row: 2,
};
pub(super) const WS_SHIP_HDEMO_SK: Col = Col {
    number: 419,
    per_row: 1,
};
pub(super) const WS_SHIP_ADDR_SK: Col = Col {
    number: 420,
    per_row: 1,
};
pub(super) const WS_WEB_PAGE_SK: Col = Col {
    number: 421,
    per_row: 16,
};
pub(super) const WS_WEB_SITE_SK: Col = Col {
    number: 422,
    per_row: 16,
};
pub(super) const WS_SHIP_MODE_SK: Col = Col {
    number: 423,
    per_row: 16,
};
pub(super) const WS_WAREHOUSE_SK: Col = Col {
    number: 424,
    per_row: 16,
};
pub(super) const WS_PROMO_SK: Col = Col {
    number: 425,
    per_row: 16,
};
pub(super) const WS_ORDER_NUMBER: Col = Col {
    number: 426,
    per_row: 1,
};
pub(super) const WS_PRICING_QUANTITY: Col = Col {
    number: 427,
    per_row: 1,
};
pub(super) const WS_PRICING_WHOLESALE_COST: Col = Col {
    number: 428,
    per_row: 1,
};
pub(super) const WS_PRICING_LIST_PRICE: Col = Col {
    number: 429,
    per_row: 0,
};
pub(super) const WS_PRICING_SALES_PRICE: Col = Col {
    number: 430,
    per_row: 0,
};
pub(super) const WS_PRICING_EXT_DISCOUNT_AMT: Col = Col {
    number: 431,
    per_row: 0,
};
pub(super) const WS_PRICING_EXT_SALES_PRICE: Col = Col {
    number: 432,
    per_row: 0,
};
pub(super) const WS_PRICING_EXT_WHOLESALE_COST: Col = Col {
    number: 433,
    per_row: 0,
};
pub(super) const WS_PRICING_EXT_LIST_PRICE: Col = Col {
    number: 434,
    per_row: 0,
};
pub(super) const WS_PRICING_EXT_TAX: Col = Col {
    number: 435,
    per_row: 0,
};
pub(super) const WS_PRICING_COUPON_AMT: Col = Col {
    number: 436,
    per_row: 0,
};
pub(super) const WS_PRICING_EXT_SHIP_COST: Col = Col {
    number: 437,
    per_row: 0,
};
pub(super) const WS_PRICING_NET_PAID: Col = Col {
    number: 438,
    per_row: 0,
};
pub(super) const WS_PRICING_NET_PAID_INC_TAX: Col = Col {
    number: 439,
    per_row: 0,
};
pub(super) const WS_PRICING_NET_PAID_INC_SHIP: Col = Col {
    number: 440,
    per_row: 0,
};
pub(super) const WS_PRICING_NET_PAID_INC_SHIP_TAX: Col = Col {
    number: 441,
    per_row: 0,
};
pub(super) const WS_PRICING_NET_PROFIT: Col = Col {
    number: 442,
    per_row: 0,
};
pub(super) const WS_PRICING: Col = Col {
    number: 443,
    per_row: 128,
};
pub(super) const WS_NULLS: Col = Col {
    number: 444,
    per_row: 32,
};
pub(super) const WR_IS_RETURNED: Col = Col {
    number: 445,
    per_row: 16,
};
pub(super) const WS_PERMUTATION: Col = Col {
    number: 446,
    per_row: 0,
};
pub(super) const WEB_SITE_SK: Col = Col {
    number: 447,
    per_row: 1,
};
pub(super) const WEB_SITE_ID: Col = Col {
    number: 448,
    per_row: 1,
};
pub(super) const WEB_REC_START_DATE_ID: Col = Col {
    number: 449,
    per_row: 1,
};
pub(super) const WEB_REC_END_DATE_ID: Col = Col {
    number: 450,
    per_row: 1,
};
pub(super) const WEB_NAME: Col = Col {
    number: 451,
    per_row: 1,
};
pub(super) const WEB_OPEN_DATE: Col = Col {
    number: 452,
    per_row: 1,
};
pub(super) const WEB_CLOSE_DATE: Col = Col {
    number: 453,
    per_row: 1,
};
pub(super) const WEB_CLASS: Col = Col {
    number: 454,
    per_row: 1,
};
pub(super) const WEB_MANAGER: Col = Col {
    number: 455,
    per_row: 2,
};
pub(super) const WEB_MARKET_ID: Col = Col {
    number: 456,
    per_row: 1,
};
pub(super) const WEB_MARKET_CLASS: Col = Col {
    number: 457,
    per_row: 20,
};
pub(super) const WEB_MARKET_DESC: Col = Col {
    number: 458,
    per_row: 100,
};
pub(super) const WEB_MARKET_MANAGER: Col = Col {
    number: 459,
    per_row: 2,
};
pub(super) const WEB_COMPANY_ID: Col = Col {
    number: 460,
    per_row: 1,
};
pub(super) const WEB_COMPANY_NAME: Col = Col {
    number: 461,
    per_row: 1,
};
pub(super) const WEB_ADDRESS_STREET_NUM: Col = Col {
    number: 462,
    per_row: 1,
};
pub(super) const WEB_ADDRESS_STREET_NAME1: Col = Col {
    number: 463,
    per_row: 1,
};
pub(super) const WEB_ADDRESS_STREET_TYPE: Col = Col {
    number: 464,
    per_row: 1,
};
pub(super) const WEB_ADDRESS_SUITE_NUM: Col = Col {
    number: 465,
    per_row: 1,
};
pub(super) const WEB_ADDRESS_CITY: Col = Col {
    number: 466,
    per_row: 1,
};
pub(super) const WEB_ADDRESS_COUNTY: Col = Col {
    number: 467,
    per_row: 1,
};
pub(super) const WEB_ADDRESS_STATE: Col = Col {
    number: 468,
    per_row: 1,
};
pub(super) const WEB_ADDRESS_ZIP: Col = Col {
    number: 469,
    per_row: 1,
};
pub(super) const WEB_ADDRESS_COUNTRY: Col = Col {
    number: 470,
    per_row: 1,
};
pub(super) const WEB_ADDRESS_GMT_OFFSET: Col = Col {
    number: 471,
    per_row: 1,
};
pub(super) const WEB_TAX_PERCENTAGE: Col = Col {
    number: 472,
    per_row: 1,
};
pub(super) const WEB_NULLS: Col = Col {
    number: 473,
    per_row: 2,
};
pub(super) const WEB_ADDRESS: Col = Col {
    number: 474,
    per_row: 7,
};
pub(super) const WEB_SCD: Col = Col {
    number: 475,
    per_row: 70,
};
pub(super) const DV_VERSION: Col = Col {
    number: 476,
    per_row: 0,
};
pub(super) const DV_CREATE_DATE: Col = Col {
    number: 477,
    per_row: 0,
};
pub(super) const DV_CREATE_TIME: Col = Col {
    number: 478,
    per_row: 0,
};
pub(super) const DV_CMDLINE_ARGS: Col = Col {
    number: 479,
    per_row: 0,
};
pub(super) const VALIDATE_STREAM: Col = Col {
    number: 480,
    per_row: 0,
};
pub(super) const S_BRAND_ID: Col = Col {
    number: 481,
    per_row: 0,
};
pub(super) const S_BRAND_SUBCLASS_ID: Col = Col {
    number: 482,
    per_row: 0,
};
pub(super) const S_BRAND_MANAGER_ID: Col = Col {
    number: 483,
    per_row: 1,
};
pub(super) const S_BRAND_MANUFACTURER_ID: Col = Col {
    number: 484,
    per_row: 1,
};
pub(super) const S_BRAND_NAME: Col = Col {
    number: 485,
    per_row: 6,
};
pub(super) const S_CADR_ID: Col = Col {
    number: 486,
    per_row: 0,
};
pub(super) const S_CADR_ADDRESS_STREET_NUMBER: Col = Col {
    number: 487,
    per_row: 1,
};
pub(super) const S_CADR_ADDRESS_STREET_NAME1: Col = Col {
    number: 488,
    per_row: 1,
};
pub(super) const S_CADR_ADDRESS_STREET_NAME2: Col = Col {
    number: 489,
    per_row: 1,
};
pub(super) const S_CADR_ADDRESS_STREET_TYPE: Col = Col {
    number: 490,
    per_row: 1,
};
pub(super) const S_CADR_ADDRESS_SUITE_NUM: Col = Col {
    number: 491,
    per_row: 1,
};
pub(super) const S_CADR_ADDRESS_CITY: Col = Col {
    number: 492,
    per_row: 1,
};
pub(super) const S_CADR_ADDRESS_COUNTY: Col = Col {
    number: 493,
    per_row: 1,
};
pub(super) const S_CADR_ADDRESS_STATE: Col = Col {
    number: 494,
    per_row: 1,
};
pub(super) const S_CADR_ADDRESS_ZIP: Col = Col {
    number: 495,
    per_row: 1,
};
pub(super) const S_CADR_ADDRESS_COUNTRY: Col = Col {
    number: 496,
    per_row: 1,
};
pub(super) const S_BADDR_ADDRESS: Col = Col {
    number: 497,
    per_row: 7,
};
pub(super) const S_CALL_CENTER_ID: Col = Col {
    number: 498,
    per_row: 1,
};
pub(super) const S_CALL_CENTER_DIVISION_ID: Col = Col {
    number: 499,
    per_row: 1,
};
pub(super) const S_CALL_CENTER_OPEN_DATE: Col = Col {
    number: 500,
    per_row: 1,
};
pub(super) const S_CALL_CENTER_CLOSED_DATE: Col = Col {
    number: 501,
    per_row: 0,
};
pub(super) const S_CALL_CENTER_NAME: Col = Col {
    number: 502,
    per_row: 0,
};
pub(super) const S_CALL_CENTER_CLASS: Col = Col {
    number: 503,
    per_row: 0,
};
pub(super) const S_CALL_CENTER_EMPLOYEES: Col = Col {
    number: 504,
    per_row: 1,
};
pub(super) const S_CALL_CENTER_SQFT: Col = Col {
    number: 505,
    per_row: 1,
};
pub(super) const S_CALL_CENTER_HOURS: Col = Col {
    number: 506,
    per_row: 1,
};
pub(super) const S_CALL_CENTER_MANAGER_ID: Col = Col {
    number: 507,
    per_row: 1,
};
pub(super) const S_CALL_CENTER_MARKET_ID: Col = Col {
    number: 508,
    per_row: 1,
};
pub(super) const S_CALL_CENTER_ADDRESS_ID: Col = Col {
    number: 509,
    per_row: 1,
};
pub(super) const S_CALL_CENTER_TAX_PERCENTAGE: Col = Col {
    number: 510,
    per_row: 1,
};
pub(super) const S_CALL_CENTER_SCD: Col = Col {
    number: 511,
    per_row: 1,
};
pub(super) const S_CATALOG_NUMBER: Col = Col {
    number: 512,
    per_row: 0,
};
pub(super) const S_CATALOG_START_DATE: Col = Col {
    number: 513,
    per_row: 1,
};
pub(super) const S_CATALOG_END_DATE: Col = Col {
    number: 514,
    per_row: 1,
};
pub(super) const S_CATALOG_DESC: Col = Col {
    number: 515,
    per_row: 10,
};
pub(super) const S_CATALOG_TYPE: Col = Col {
    number: 516,
    per_row: 1,
};
pub(super) const S_CORD_ID: Col = Col {
    number: 517,
    per_row: 0,
};
pub(super) const S_CORD_BILL_CUSTOMER_ID: Col = Col {
    number: 518,
    per_row: 1,
};
pub(super) const S_CORD_SHIP_CUSTOMER_ID: Col = Col {
    number: 519,
    per_row: 2,
};
pub(super) const S_CORD_ORDER_DATE: Col = Col {
    number: 520,
    per_row: 1,
};
pub(super) const S_CORD_ORDER_TIME: Col = Col {
    number: 521,
    per_row: 1,
};
pub(super) const S_CORD_SHIP_MODE_ID: Col = Col {
    number: 522,
    per_row: 1,
};
pub(super) const S_CORD_CALL_CENTER_ID: Col = Col {
    number: 523,
    per_row: 1,
};
pub(super) const S_CLIN_ITEM_ID: Col = Col {
    number: 524,
    per_row: 1,
};
pub(super) const S_CORD_COMMENT: Col = Col {
    number: 525,
    per_row: 100,
};
pub(super) const S_CLIN_ORDER_ID: Col = Col {
    number: 526,
    per_row: 1,
};
pub(super) const S_CLIN_LINE_NUMBER: Col = Col {
    number: 527,
    per_row: 0,
};
pub(super) const S_CLIN_PROMOTION_ID: Col = Col {
    number: 528,
    per_row: 1,
};
pub(super) const S_CLIN_QUANTITY: Col = Col {
    number: 529,
    per_row: 1,
};
pub(super) const S_CLIN_COUPON_AMT: Col = Col {
    number: 530,
    per_row: 1,
};
pub(super) const S_CLIN_WAREHOUSE_ID: Col = Col {
    number: 531,
    per_row: 1,
};
pub(super) const S_CLIN_SHIP_DATE: Col = Col {
    number: 532,
    per_row: 1,
};
pub(super) const S_CLIN_CATALOG_ID: Col = Col {
    number: 533,
    per_row: 1,
};
pub(super) const S_CLIN_CATALOG_PAGE_ID: Col = Col {
    number: 534,
    per_row: 2,
};
pub(super) const S_CLIN_PRICING: Col = Col {
    number: 535,
    per_row: 8,
};
pub(super) const S_CLIN_SHIP_COST: Col = Col {
    number: 536,
    per_row: 0,
};
pub(super) const S_CLIN_IS_RETURNED: Col = Col {
    number: 537,
    per_row: 1,
};
pub(super) const S_CLIN_PERMUTE: Col = Col {
    number: 538,
    per_row: 0,
};
pub(super) const S_CATALOG_PAGE_CATALOG_NUMBER: Col = Col {
    number: 539,
    per_row: 0,
};
pub(super) const S_CATALOG_PAGE_NUMBER: Col = Col {
    number: 540,
    per_row: 0,
};
pub(super) const S_CATALOG_PAGE_DEPARTMENT: Col = Col {
    number: 541,
    per_row: 1,
};
pub(super) const S_CP_ID: Col = Col {
    number: 542,
    per_row: 1,
};
pub(super) const S_CP_START_DATE: Col = Col {
    number: 543,
    per_row: 1,
};
pub(super) const S_CP_END_DATE: Col = Col {
    number: 544,
    per_row: 1,
};
pub(super) const S_CP_DESCRIPTION: Col = Col {
    number: 545,
    per_row: 100,
};
pub(super) const S_CP_TYPE: Col = Col {
    number: 546,
    per_row: 1,
};
pub(super) const S_CATALOG_PROMOTIONAL_ITEM_CATALOG_NUMBER: Col = Col {
    number: 547,
    per_row: 1,
};
pub(super) const S_CATALOG_PROMOTIONAL_ITEM_CATALOG_PAGE_NUMBER: Col = Col {
    number: 548,
    per_row: 1,
};
pub(super) const S_CATALOG_PROMOTIONAL_ITEM_ITEM_ID: Col = Col {
    number: 549,
    per_row: 1,
};
pub(super) const S_CATALOG_PROMOTIONAL_ITEM_PROMOTION_ID: Col = Col {
    number: 550,
    per_row: 0,
};
pub(super) const S_CRET_CALL_CENTER_ID: Col = Col {
    number: 551,
    per_row: 9,
};
pub(super) const S_CRET_ORDER_ID: Col = Col {
    number: 552,
    per_row: 0,
};
pub(super) const S_CRET_LINE_NUMBER: Col = Col {
    number: 553,
    per_row: 0,
};
pub(super) const S_CRET_ITEM_ID: Col = Col {
    number: 554,
    per_row: 0,
};
pub(super) const S_CRET_RETURN_CUSTOMER_ID: Col = Col {
    number: 555,
    per_row: 0,
};
pub(super) const S_CRET_REFUND_CUSTOMER_ID: Col = Col {
    number: 556,
    per_row: 0,
};
pub(super) const S_CRET_DATE: Col = Col {
    number: 557,
    per_row: 9,
};
pub(super) const S_CRET_TIME: Col = Col {
    number: 558,
    per_row: 18,
};
pub(super) const S_CRET_QUANTITY: Col = Col {
    number: 559,
    per_row: 0,
};
pub(super) const S_CRET_AMOUNT: Col = Col {
    number: 560,
    per_row: 0,
};
pub(super) const S_CRET_TAX: Col = Col {
    number: 561,
    per_row: 0,
};
pub(super) const S_CRET_FEE: Col = Col {
    number: 562,
    per_row: 0,
};
pub(super) const S_CRET_SHIP_COST: Col = Col {
    number: 563,
    per_row: 0,
};
pub(super) const S_CRET_REFUNDED_CASH: Col = Col {
    number: 564,
    per_row: 0,
};
pub(super) const S_CRET_REVERSED_CHARGE: Col = Col {
    number: 565,
    per_row: 0,
};
pub(super) const S_CRET_MERCHANT_CREDIT: Col = Col {
    number: 566,
    per_row: 0,
};
pub(super) const S_CRET_REASON_ID: Col = Col {
    number: 567,
    per_row: 9,
};
pub(super) const S_CRET_PRICING: Col = Col {
    number: 568,
    per_row: 72,
};
pub(super) const S_CRET_SHIPMODE_ID: Col = Col {
    number: 569,
    per_row: 9,
};
pub(super) const S_CRET_WAREHOUSE_ID: Col = Col {
    number: 570,
    per_row: 9,
};
pub(super) const S_CRET_CATALOG_PAGE_ID: Col = Col {
    number: 571,
    per_row: 0,
};
pub(super) const S_CATEGORY_ID: Col = Col {
    number: 572,
    per_row: 0,
};
pub(super) const S_CATEGORY_NAME: Col = Col {
    number: 573,
    per_row: 0,
};
pub(super) const S_CATEGORY_DESC: Col = Col {
    number: 574,
    per_row: 10,
};
pub(super) const S_CLASS_ID: Col = Col {
    number: 575,
    per_row: 0,
};
pub(super) const S_CLASS_SUBCAT_ID: Col = Col {
    number: 576,
    per_row: 1,
};
pub(super) const S_CLASS_DESC: Col = Col {
    number: 577,
    per_row: 10,
};
pub(super) const S_COMPANY_ID: Col = Col {
    number: 578,
    per_row: 0,
};
pub(super) const S_COMPANY_NAME: Col = Col {
    number: 579,
    per_row: 0,
};
pub(super) const S_CUST_ID: Col = Col {
    number: 580,
    per_row: 0,
};
pub(super) const S_CUST_SALUTATION: Col = Col {
    number: 581,
    per_row: 1,
};
pub(super) const S_CUST_LAST_NAME: Col = Col {
    number: 582,
    per_row: 1,
};
pub(super) const S_CUST_FIRST_NAME: Col = Col {
    number: 583,
    per_row: 1,
};
pub(super) const S_CUST_PREFERRED_FLAG: Col = Col {
    number: 584,
    per_row: 1,
};
pub(super) const S_CUST_BIRTH_DATE: Col = Col {
    number: 585,
    per_row: 1,
};
pub(super) const S_CUST_FIRST_PURCHASE_DATE: Col = Col {
    number: 586,
    per_row: 1,
};
pub(super) const S_CUST_FIRST_SHIPTO_DATE: Col = Col {
    number: 587,
    per_row: 1,
};
pub(super) const S_CUST_BIRTH_COUNTRY: Col = Col {
    number: 588,
    per_row: 1,
};
pub(super) const S_CUST_LOGIN: Col = Col {
    number: 589,
    per_row: 25,
};
pub(super) const S_CUST_EMAIL: Col = Col {
    number: 590,
    per_row: 23,
};
pub(super) const S_CUST_LAST_LOGIN: Col = Col {
    number: 591,
    per_row: 1,
};
pub(super) const S_CUST_LAST_REVIEW: Col = Col {
    number: 592,
    per_row: 1,
};
pub(super) const S_CUST_PRIMARY_MACHINE: Col = Col {
    number: 593,
    per_row: 4,
};
pub(super) const S_CUST_SECONDARY_MACHINE: Col = Col {
    number: 594,
    per_row: 4,
};
pub(super) const S_CUST_ADDRESS: Col = Col {
    number: 595,
    per_row: 7,
};
pub(super) const S_CUST_ADDRESS_STREET_NUM: Col = Col {
    number: 596,
    per_row: 1,
};
pub(super) const S_CUST_ADDRESS_STREET_NAME1: Col = Col {
    number: 597,
    per_row: 1,
};
pub(super) const S_CUST_ADDRESS_STREET_NAME2: Col = Col {
    number: 598,
    per_row: 1,
};
pub(super) const S_CUST_ADDRESS_STREET_TYPE: Col = Col {
    number: 599,
    per_row: 1,
};
pub(super) const S_CUST_ADDRESS_SUITE_NUM: Col = Col {
    number: 600,
    per_row: 1,
};
pub(super) const S_CUST_ADDRESS_CITY: Col = Col {
    number: 601,
    per_row: 1,
};
pub(super) const S_CUST_ADDRESS_ZIP: Col = Col {
    number: 602,
    per_row: 1,
};
pub(super) const S_CUST_ADDRESS_COUNTY: Col = Col {
    number: 603,
    per_row: 1,
};
pub(super) const S_CUST_ADDRESS_STATE: Col = Col {
    number: 604,
    per_row: 1,
};
pub(super) const S_CUST_ADDRESS_COUNTRY: Col = Col {
    number: 605,
    per_row: 1,
};
pub(super) const S_CUST_LOCATION_TYPE: Col = Col {
    number: 606,
    per_row: 1,
};
pub(super) const S_CUST_GENDER: Col = Col {
    number: 607,
    per_row: 1,
};
pub(super) const S_CUST_MARITAL_STATUS: Col = Col {
    number: 608,
    per_row: 1,
};
pub(super) const S_CUST_EDUCATION: Col = Col {
    number: 609,
    per_row: 1,
};
pub(super) const S_CUST_CREDIT_RATING: Col = Col {
    number: 610,
    per_row: 1,
};
pub(super) const S_CUST_PURCHASE_ESTIMATE: Col = Col {
    number: 611,
    per_row: 1,
};
pub(super) const S_CUST_BUY_POTENTIAL: Col = Col {
    number: 612,
    per_row: 1,
};
pub(super) const S_CUST_DEPENDENT_CNT: Col = Col {
    number: 613,
    per_row: 1,
};
pub(super) const S_CUST_EMPLOYED_CNT: Col = Col {
    number: 614,
    per_row: 1,
};
pub(super) const S_CUST_COLLEGE_CNT: Col = Col {
    number: 615,
    per_row: 1,
};
pub(super) const S_CUST_VEHICLE_CNT: Col = Col {
    number: 616,
    per_row: 1,
};
pub(super) const S_CUST_INCOME: Col = Col {
    number: 617,
    per_row: 1,
};
pub(super) const S_DIVISION_ID: Col = Col {
    number: 618,
    per_row: 0,
};
pub(super) const S_DIVISION_COMPANY: Col = Col {
    number: 619,
    per_row: 0,
};
pub(super) const S_DIVISION_NAME: Col = Col {
    number: 620,
    per_row: 0,
};
pub(super) const S_INVN_WAREHOUSE: Col = Col {
    number: 621,
    per_row: 1,
};
pub(super) const S_INVN_ITEM: Col = Col {
    number: 622,
    per_row: 1,
};
pub(super) const S_INVN_DATE: Col = Col {
    number: 623,
    per_row: 2,
};
pub(super) const S_INVN_QUANTITY: Col = Col {
    number: 624,
    per_row: 1,
};
pub(super) const S_ITEM_ID: Col = Col {
    number: 625,
    per_row: 1,
};
pub(super) const S_ITEM_PERMUTE: Col = Col {
    number: 626,
    per_row: 0,
};
pub(super) const S_ITEM_PRODUCT_ID: Col = Col {
    number: 627,
    per_row: 1,
};
pub(super) const S_ITEM_DESC: Col = Col {
    number: 628,
    per_row: 200,
};
pub(super) const S_ITEM_LIST_PRICE: Col = Col {
    number: 629,
    per_row: 2,
};
pub(super) const S_ITEM_WHOLESALE_COST: Col = Col {
    number: 630,
    per_row: 1,
};
pub(super) const S_ITEM_MANAGER_ID: Col = Col {
    number: 631,
    per_row: 1,
};
pub(super) const S_ITEM_SIZE: Col = Col {
    number: 632,
    per_row: 1,
};
pub(super) const S_ITEM_FORMULATION: Col = Col {
    number: 633,
    per_row: 50,
};
pub(super) const S_ITEM_FLAVOR: Col = Col {
    number: 634,
    per_row: 1,
};
pub(super) const S_ITEM_UNITS: Col = Col {
    number: 635,
    per_row: 1,
};
pub(super) const S_ITEM_CONTAINER: Col = Col {
    number: 636,
    per_row: 1,
};
pub(super) const S_ITEM_SCD: Col = Col {
    number: 637,
    per_row: 1,
};
pub(super) const S_MANAGER_ID: Col = Col {
    number: 638,
    per_row: 0,
};
pub(super) const S_MANAGER_NAME: Col = Col {
    number: 639,
    per_row: 2,
};
pub(super) const S_MANUFACTURER_ID: Col = Col {
    number: 640,
    per_row: 0,
};
pub(super) const S_MANUFACTURER_NAME: Col = Col {
    number: 641,
    per_row: 0,
};
pub(super) const S_MARKET_ID: Col = Col {
    number: 642,
    per_row: 0,
};
pub(super) const S_MARKET_CLASS_NAME: Col = Col {
    number: 643,
    per_row: 0,
};
pub(super) const S_MARKET_DESC: Col = Col {
    number: 644,
    per_row: 10,
};
pub(super) const S_MARKET_MANAGER_ID: Col = Col {
    number: 645,
    per_row: 1,
};
pub(super) const S_PRODUCT_ID: Col = Col {
    number: 646,
    per_row: 0,
};
pub(super) const S_PRODUCT_BRAND_ID: Col = Col {
    number: 647,
    per_row: 1,
};
pub(super) const S_PRODUCT_NAME: Col = Col {
    number: 648,
    per_row: 0,
};
pub(super) const S_PRODUCT_TYPE: Col = Col {
    number: 649,
    per_row: 1,
};
pub(super) const S_PROMOTION_ID: Col = Col {
    number: 650,
    per_row: 1,
};
pub(super) const S_PROMOTION_ITEM_ID: Col = Col {
    number: 651,
    per_row: 1,
};
pub(super) const S_PROMOTION_START_DATE: Col = Col {
    number: 652,
    per_row: 1,
};
pub(super) const S_PROMOTION_END_DATE: Col = Col {
    number: 653,
    per_row: 1,
};
pub(super) const S_PROMOTION_COST: Col = Col {
    number: 654,
    per_row: 1,
};
pub(super) const S_PROMOTION_RESPONSE_TARGET: Col = Col {
    number: 655,
    per_row: 1,
};
pub(super) const S_PROMOTION_DMAIL: Col = Col {
    number: 656,
    per_row: 0,
};
pub(super) const S_PROMOTION_EMAIL: Col = Col {
    number: 657,
    per_row: 0,
};
pub(super) const S_PROMOTION_CATALOG: Col = Col {
    number: 658,
    per_row: 0,
};
pub(super) const S_PROMOTION_TV: Col = Col {
    number: 659,
    per_row: 0,
};
pub(super) const S_PROMOTION_RADIO: Col = Col {
    number: 660,
    per_row: 0,
};
pub(super) const S_PROMOTION_PRESS: Col = Col {
    number: 661,
    per_row: 0,
};
pub(super) const S_PROMOTION_EVENT: Col = Col {
    number: 662,
    per_row: 0,
};
pub(super) const S_PROMOTION_DEMO: Col = Col {
    number: 663,
    per_row: 0,
};
pub(super) const S_PROMOTION_DETAILS: Col = Col {
    number: 664,
    per_row: 100,
};
pub(super) const S_PROMOTION_PURPOSE: Col = Col {
    number: 665,
    per_row: 1,
};
pub(super) const S_PROMOTION_DISCOUNT_ACTIVE: Col = Col {
    number: 666,
    per_row: 1,
};
pub(super) const S_PROMOTION_DISCOUNT_PCT: Col = Col {
    number: 667,
    per_row: 1,
};
pub(super) const S_PROMOTION_NAME: Col = Col {
    number: 668,
    per_row: 0,
};
pub(super) const S_PROMOTION_BITFIELD: Col = Col {
    number: 669,
    per_row: 1,
};
pub(super) const S_PURCHASE_ID: Col = Col {
    number: 670,
    per_row: 0,
};
pub(super) const S_PURCHASE_STORE_ID: Col = Col {
    number: 671,
    per_row: 1,
};
pub(super) const S_PURCHASE_CUSTOMER_ID: Col = Col {
    number: 672,
    per_row: 1,
};
pub(super) const S_PURCHASE_DATE: Col = Col {
    number: 673,
    per_row: 1,
};
pub(super) const S_PURCHASE_TIME: Col = Col {
    number: 674,
    per_row: 1,
};
pub(super) const S_PURCHASE_REGISTER: Col = Col {
    number: 675,
    per_row: 1,
};
pub(super) const S_PURCHASE_CLERK: Col = Col {
    number: 676,
    per_row: 1,
};
pub(super) const S_PURCHASE_COMMENT: Col = Col {
    number: 677,
    per_row: 100,
};
pub(super) const S_PURCHASE_PRICING: Col = Col {
    number: 678,
    per_row: 7,
};
pub(super) const S_PLINE_ITEM_ID: Col = Col {
    number: 679,
    per_row: 1,
};
pub(super) const S_PLINE_PURCHASE_ID: Col = Col {
    number: 680,
    per_row: 12,
};
pub(super) const S_PLINE_NUMBER: Col = Col {
    number: 681,
    per_row: 12,
};
pub(super) const S_PLINE_PROMOTION_ID: Col = Col {
    number: 682,
    per_row: 12,
};
pub(super) const S_PLINE_SALE_PRICE: Col = Col {
    number: 683,
    per_row: 12,
};
pub(super) const S_PLINE_QUANTITY: Col = Col {
    number: 684,
    per_row: 12,
};
pub(super) const S_PLINE_COUPON_AMT: Col = Col {
    number: 685,
    per_row: 12,
};
pub(super) const S_PLINE_COMMENT: Col = Col {
    number: 686,
    per_row: 1200,
};
pub(super) const S_PLINE_PRICING: Col = Col {
    number: 687,
    per_row: 96,
};
pub(super) const S_PLINE_IS_RETURNED: Col = Col {
    number: 688,
    per_row: 12,
};
pub(super) const S_PLINE_PERMUTE: Col = Col {
    number: 689,
    per_row: 0,
};
pub(super) const S_REASON_ID: Col = Col {
    number: 690,
    per_row: 0,
};
pub(super) const S_REASON_DESC: Col = Col {
    number: 691,
    per_row: 10,
};
pub(super) const S_STORE_ID: Col = Col {
    number: 692,
    per_row: 1,
};
pub(super) const S_STORE_ADDRESS_ID: Col = Col {
    number: 693,
    per_row: 1,
};
pub(super) const S_STORE_DIVISION_ID: Col = Col {
    number: 694,
    per_row: 1,
};
pub(super) const S_STORE_OPEN_DATE: Col = Col {
    number: 695,
    per_row: 1,
};
pub(super) const S_STORE_CLOSE_DATE: Col = Col {
    number: 696,
    per_row: 1,
};
pub(super) const S_STORE_NAME: Col = Col {
    number: 697,
    per_row: 0,
};
pub(super) const S_STORE_CLASS: Col = Col {
    number: 698,
    per_row: 0,
};
pub(super) const S_STORE_EMPLOYEES: Col = Col {
    number: 699,
    per_row: 1,
};
pub(super) const S_STORE_FLOOR_SPACE: Col = Col {
    number: 700,
    per_row: 1,
};
pub(super) const S_STORE_HOURS: Col = Col {
    number: 701,
    per_row: 1,
};
pub(super) const S_STORE_MARKET_MANAGER_ID: Col = Col {
    number: 702,
    per_row: 0,
};
pub(super) const S_STORE_MANAGER_ID: Col = Col {
    number: 703,
    per_row: 1,
};
pub(super) const S_STORE_MARKET_ID: Col = Col {
    number: 704,
    per_row: 1,
};
pub(super) const S_STORE_GEOGRAPHY_CLASS: Col = Col {
    number: 705,
    per_row: 1,
};
pub(super) const S_STORE_TAX_PERCENTAGE: Col = Col {
    number: 706,
    per_row: 1,
};
pub(super) const S_SITM_PROMOTION_ID: Col = Col {
    number: 707,
    per_row: 1,
};
pub(super) const S_SITM_ITEM_ID: Col = Col {
    number: 708,
    per_row: 1,
};
pub(super) const S_SITM_STORE_ID: Col = Col {
    number: 709,
    per_row: 1,
};
pub(super) const S_SRET_STORE_ID: Col = Col {
    number: 710,
    per_row: 0,
};
pub(super) const S_SRET_PURCHASE_ID: Col = Col {
    number: 711,
    per_row: 0,
};
pub(super) const S_SRET_LINENUMBER: Col = Col {
    number: 712,
    per_row: 0,
};
pub(super) const S_SRET_ITEM_ID: Col = Col {
    number: 713,
    per_row: 0,
};
pub(super) const S_SRET_CUSTOMER_ID: Col = Col {
    number: 714,
    per_row: 0,
};
pub(super) const S_SRET_RETURN_DATE: Col = Col {
    number: 715,
    per_row: 24,
};
pub(super) const S_SRET_RETURN_TIME: Col = Col {
    number: 716,
    per_row: 12,
};
pub(super) const S_SRET_TICKET_NUMBER: Col = Col {
    number: 717,
    per_row: 0,
};
pub(super) const S_SRET_RETURN_QUANTITY: Col = Col {
    number: 718,
    per_row: 0,
};
pub(super) const S_SRET_RETURN_AMT: Col = Col {
    number: 719,
    per_row: 0,
};
pub(super) const S_SRET_RETURN_TAX: Col = Col {
    number: 720,
    per_row: 0,
};
pub(super) const S_SRET_RETURN_FEE: Col = Col {
    number: 721,
    per_row: 0,
};
pub(super) const S_SRET_RETURN_SHIP_COST: Col = Col {
    number: 722,
    per_row: 0,
};
pub(super) const S_SRET_REFUNDED_CASH: Col = Col {
    number: 723,
    per_row: 0,
};
pub(super) const S_SRET_REVERSED_CHARGE: Col = Col {
    number: 724,
    per_row: 0,
};
pub(super) const S_SRET_MERCHANT_CREDIT: Col = Col {
    number: 725,
    per_row: 0,
};
pub(super) const S_SRET_REASON_ID: Col = Col {
    number: 726,
    per_row: 12,
};
pub(super) const S_SRET_PRICING: Col = Col {
    number: 727,
    per_row: 84,
};
pub(super) const S_SBCT_ID: Col = Col {
    number: 728,
    per_row: 0,
};
pub(super) const S_SBCT_CATEGORY_ID: Col = Col {
    number: 729,
    per_row: 1,
};
pub(super) const S_SBCT_NAME: Col = Col {
    number: 730,
    per_row: 0,
};
pub(super) const S_SBCT_DESC: Col = Col {
    number: 731,
    per_row: 10,
};
pub(super) const S_SUBC_ID: Col = Col {
    number: 732,
    per_row: 0,
};
pub(super) const S_SUBC_CLASS_ID: Col = Col {
    number: 733,
    per_row: 1,
};
pub(super) const S_SUBC_NAME: Col = Col {
    number: 734,
    per_row: 0,
};
pub(super) const S_SUBC_DESC: Col = Col {
    number: 735,
    per_row: 10,
};
pub(super) const S_WRHS_ID: Col = Col {
    number: 736,
    per_row: 1,
};
pub(super) const S_WRHS_DESC: Col = Col {
    number: 737,
    per_row: 10,
};
pub(super) const S_WRHS_SQFT: Col = Col {
    number: 738,
    per_row: 1,
};
pub(super) const S_WRHS_ADDRESS_ID: Col = Col {
    number: 739,
    per_row: 1,
};
pub(super) const S_WORD_ID: Col = Col {
    number: 740,
    per_row: 1,
};
pub(super) const S_WORD_BILL_CUSTOMER_ID: Col = Col {
    number: 741,
    per_row: 1,
};
pub(super) const S_WORD_SHIP_CUSTOMER_ID: Col = Col {
    number: 742,
    per_row: 2,
};
pub(super) const S_WORD_ORDER_DATE: Col = Col {
    number: 743,
    per_row: 1,
};
pub(super) const S_WORD_ORDER_TIME: Col = Col {
    number: 744,
    per_row: 1,
};
pub(super) const S_WORD_SHIP_MODE_ID: Col = Col {
    number: 745,
    per_row: 1,
};
pub(super) const S_WORD_WEB_SITE_ID: Col = Col {
    number: 746,
    per_row: 1,
};
pub(super) const S_WORD_COMMENT: Col = Col {
    number: 747,
    per_row: 100,
};
pub(super) const S_WLIN_ITEM_ID: Col = Col {
    number: 748,
    per_row: 1,
};
pub(super) const S_WLIN_ID: Col = Col {
    number: 749,
    per_row: 12,
};
pub(super) const S_WLIN_LINE_NUMBER: Col = Col {
    number: 750,
    per_row: 0,
};
pub(super) const S_WLIN_PROMOTION_ID: Col = Col {
    number: 751,
    per_row: 12,
};
pub(super) const S_WLIN_QUANTITY: Col = Col {
    number: 752,
    per_row: 12,
};
pub(super) const S_WLIN_COUPON_AMT: Col = Col {
    number: 753,
    per_row: 12,
};
pub(super) const S_WLIN_WAREHOUSE_ID: Col = Col {
    number: 754,
    per_row: 12,
};
pub(super) const S_WLIN_SHIP_DATE: Col = Col {
    number: 755,
    per_row: 12,
};
pub(super) const S_WLIN_WEB_PAGE_ID: Col = Col {
    number: 756,
    per_row: 12,
};
pub(super) const S_WLIN_PRICING: Col = Col {
    number: 757,
    per_row: 96,
};
pub(super) const S_WLIN_SHIP_COST: Col = Col {
    number: 758,
    per_row: 0,
};
pub(super) const S_WLIN_IS_RETURNED: Col = Col {
    number: 759,
    per_row: 12,
};
pub(super) const S_WLIN_PERMUTE: Col = Col {
    number: 760,
    per_row: 0,
};
pub(super) const S_WPAG_SITE_ID: Col = Col {
    number: 761,
    per_row: 1,
};
pub(super) const S_WPAG_ID: Col = Col {
    number: 762,
    per_row: 1,
};
pub(super) const S_WPAG_CREATE_DATE: Col = Col {
    number: 763,
    per_row: 1,
};
pub(super) const S_WPAG_ACCESS_DATE: Col = Col {
    number: 764,
    per_row: 1,
};
pub(super) const S_WPAG_AUTOGEN_FLAG: Col = Col {
    number: 765,
    per_row: 1,
};
pub(super) const S_WPAG_DEPARTMENT: Col = Col {
    number: 766,
    per_row: 1,
};
pub(super) const S_WPAG_URL: Col = Col {
    number: 767,
    per_row: 1,
};
pub(super) const S_WPAG_TYPE: Col = Col {
    number: 768,
    per_row: 1,
};
pub(super) const S_WPAG_CHAR_CNT: Col = Col {
    number: 769,
    per_row: 1,
};
pub(super) const S_WPAG_LINK_CNT: Col = Col {
    number: 770,
    per_row: 1,
};
pub(super) const S_WPAG_IMAGE_CNT: Col = Col {
    number: 771,
    per_row: 1,
};
pub(super) const S_WPAG_MAX_AD_CNT: Col = Col {
    number: 772,
    per_row: 1,
};
pub(super) const S_WPAG_PERMUTE: Col = Col {
    number: 773,
    per_row: 0,
};
pub(super) const S_WITM_SITE_ID: Col = Col {
    number: 774,
    per_row: 1,
};
pub(super) const S_WITM_PAGE_ID: Col = Col {
    number: 775,
    per_row: 1,
};
pub(super) const S_WITM_ITEM_ID: Col = Col {
    number: 776,
    per_row: 1,
};
pub(super) const S_WITM_PROMOTION_ID: Col = Col {
    number: 777,
    per_row: 1,
};
pub(super) const S_WRET_SITE_ID: Col = Col {
    number: 778,
    per_row: 0,
};
pub(super) const S_WRET_ORDER_ID: Col = Col {
    number: 779,
    per_row: 0,
};
pub(super) const S_WRET_LINE_NUMBER: Col = Col {
    number: 780,
    per_row: 0,
};
pub(super) const S_WRET_ITEM_ID: Col = Col {
    number: 781,
    per_row: 0,
};
pub(super) const S_WRET_RETURN_CUST_ID: Col = Col {
    number: 782,
    per_row: 0,
};
pub(super) const S_WRET_REFUND_CUST_ID: Col = Col {
    number: 783,
    per_row: 0,
};
pub(super) const S_WRET_RETURN_DATE: Col = Col {
    number: 784,
    per_row: 24,
};
pub(super) const S_WRET_RETURN_TIME: Col = Col {
    number: 785,
    per_row: 12,
};
pub(super) const S_WRET_REASON_ID: Col = Col {
    number: 786,
    per_row: 12,
};
pub(super) const S_WRET_PRICING: Col = Col {
    number: 787,
    per_row: 84,
};
pub(super) const S_WSIT_ID: Col = Col {
    number: 788,
    per_row: 1,
};
pub(super) const S_WSIT_OPEN_DATE: Col = Col {
    number: 789,
    per_row: 1,
};
pub(super) const S_WSIT_CLOSE_DATE: Col = Col {
    number: 790,
    per_row: 1,
};
pub(super) const S_WSIT_NAME: Col = Col {
    number: 791,
    per_row: 0,
};
pub(super) const S_WSIT_ADDRESS_ID: Col = Col {
    number: 792,
    per_row: 1,
};
pub(super) const S_WSIT_DIVISION_ID: Col = Col {
    number: 793,
    per_row: 1,
};
pub(super) const S_WSIT_CLASS: Col = Col {
    number: 794,
    per_row: 1,
};
pub(super) const S_WSIT_MANAGER_ID: Col = Col {
    number: 795,
    per_row: 1,
};
pub(super) const S_WSIT_MARKET_ID: Col = Col {
    number: 796,
    per_row: 1,
};
pub(super) const S_WSIT_TAX_PERCENTAGE: Col = Col {
    number: 797,
    per_row: 1,
};
pub(super) const S_ZIPG_ZIP: Col = Col {
    number: 798,
    per_row: 0,
};
pub(super) const S_ZIPG_GMT: Col = Col {
    number: 799,
    per_row: 0,
};

pub(super) const TABLE_CALL_CENTER: Span = Span { first: 1, last: 34 };
pub(super) const TABLE_CATALOG_PAGE: Span = Span {
    first: 35,
    last: 45,
};
pub(super) const TABLE_CATALOG_RETURNS: Span = Span {
    first: 46,
    last: 74,
};
pub(super) const TABLE_CATALOG_SALES: Span = Span {
    first: 75,
    last: 113,
};
pub(super) const TABLE_CUSTOMER: Span = Span {
    first: 114,
    last: 132,
};
pub(super) const TABLE_CUSTOMER_ADDRESS: Span = Span {
    first: 133,
    last: 148,
};
pub(super) const TABLE_CUSTOMER_DEMOGRAPHICS: Span = Span {
    first: 149,
    last: 158,
};
pub(super) const TABLE_DATE: Span = Span {
    first: 159,
    last: 187,
};
pub(super) const TABLE_HOUSEHOLD_DEMOGRAPHICS: Span = Span {
    first: 188,
    last: 193,
};
pub(super) const TABLE_INCOME_BAND: Span = Span {
    first: 194,
    last: 197,
};
pub(super) const TABLE_INVENTORY: Span = Span {
    first: 198,
    last: 202,
};
pub(super) const TABLE_ITEM: Span = Span {
    first: 203,
    last: 227,
};
pub(super) const TABLE_PROMOTION: Span = Span {
    first: 228,
    last: 247,
};
pub(super) const TABLE_REASON: Span = Span {
    first: 248,
    last: 251,
};
pub(super) const TABLE_SHIP_MODE: Span = Span {
    first: 252,
    last: 258,
};
pub(super) const TABLE_STORE: Span = Span {
    first: 259,
    last: 291,
};
pub(super) const TABLE_STORE_RETURNS: Span = Span {
    first: 292,
    last: 313,
};
pub(super) const TABLE_STORE_SALES: Span = Span {
    first: 314,
    last: 339,
};
pub(super) const TABLE_TIME: Span = Span {
    first: 340,
    last: 350,
};
pub(super) const TABLE_WAREHOUSE: Span = Span {
    first: 351,
    last: 366,
};
pub(super) const TABLE_WEB_PAGE: Span = Span {
    first: 367,
    last: 382,
};
pub(super) const TABLE_WEB_RETURNS: Span = Span {
    first: 383,
    last: 408,
};
pub(super) const TABLE_WEB_SALES: Span = Span {
    first: 409,
    last: 446,
};
pub(super) const TABLE_WEB_SITE: Span = Span {
    first: 447,
    last: 475,
};
pub(super) const TABLE_DBGEN_VERSION: Span = Span {
    first: 476,
    last: 480,
};
pub(super) const TABLE_S_BRAND: Span = Span {
    first: 481,
    last: 485,
};
pub(super) const TABLE_S_CUSTOMER_ADDRESS: Span = Span {
    first: 486,
    last: 497,
};
pub(super) const TABLE_S_CALL_CENTER: Span = Span {
    first: 498,
    last: 511,
};
pub(super) const TABLE_S_CATALOG: Span = Span {
    first: 512,
    last: 516,
};
pub(super) const TABLE_S_CATALOG_ORDER: Span = Span {
    first: 517,
    last: 525,
};
pub(super) const TABLE_S_CATALOG_ORDER_LINEITEM: Span = Span {
    first: 526,
    last: 538,
};
pub(super) const TABLE_S_CATALOG_PAGE: Span = Span {
    first: 539,
    last: 546,
};
pub(super) const TABLE_S_CATALOG_PROMOTIONAL_ITEM: Span = Span {
    first: 547,
    last: 550,
};
pub(super) const TABLE_S_CATALOG_RETURNS: Span = Span {
    first: 551,
    last: 571,
};
pub(super) const TABLE_S_CATEGORY: Span = Span {
    first: 572,
    last: 574,
};
pub(super) const TABLE_S_CLASS: Span = Span {
    first: 575,
    last: 577,
};
pub(super) const TABLE_S_COMPANY: Span = Span {
    first: 578,
    last: 579,
};
pub(super) const TABLE_S_CUSTOMER: Span = Span {
    first: 580,
    last: 617,
};
pub(super) const TABLE_S_DIVISION: Span = Span {
    first: 618,
    last: 620,
};
pub(super) const TABLE_S_INVENTORY: Span = Span {
    first: 621,
    last: 624,
};
pub(super) const TABLE_S_ITEM: Span = Span {
    first: 625,
    last: 637,
};
pub(super) const TABLE_S_MANAGER: Span = Span {
    first: 638,
    last: 639,
};
pub(super) const TABLE_S_MANUFACTURER: Span = Span {
    first: 640,
    last: 641,
};
pub(super) const TABLE_S_MARKET: Span = Span {
    first: 642,
    last: 645,
};
pub(super) const TABLE_S_PRODUCT: Span = Span {
    first: 646,
    last: 649,
};
pub(super) const TABLE_S_PROMOTION: Span = Span {
    first: 650,
    last: 669,
};
pub(super) const TABLE_S_PURCHASE: Span = Span {
    first: 670,
    last: 679,
};
pub(super) const TABLE_S_PURCHASE_LINEITEM: Span = Span {
    first: 680,
    last: 689,
};
pub(super) const TABLE_S_REASON: Span = Span {
    first: 690,
    last: 691,
};
pub(super) const TABLE_S_STORE: Span = Span {
    first: 692,
    last: 706,
};
pub(super) const TABLE_S_STORE_PROMOTIONAL_ITEM: Span = Span {
    first: 707,
    last: 709,
};
pub(super) const TABLE_S_STORE_RETURNS: Span = Span {
    first: 710,
    last: 727,
};
pub(super) const TABLE_S_SUBCATEGORY: Span = Span {
    first: 728,
    last: 731,
};
pub(super) const TABLE_S_SUBCLASS: Span = Span {
    first: 732,
    last: 735,
};
pub(super) const TABLE_S_WAREHOUSE: Span = Span {
    first: 736,
    last: 739,
};
pub(super) const TABLE_S_WEB_ORDER: Span = Span {
    first: 740,
    last: 748,
};
pub(super) const TABLE_S_WEB_ORDER_LINEITEM: Span = Span {
    first: 749,
    last: 760,
};
pub(super) const TABLE_S_WEB_PAGE: Span = Span {
    first: 761,
    last: 773,
};
pub(super) const TABLE_S_WEB_PROMOTIONAL_ITEM: Span = Span {
    first: 774,
    last: 777,
};
pub(super) const TABLE_S_WEB_RETURNS: Span = Span {
    first: 778,
    last: 787,
};
pub(super) const TABLE_S_WEB_SITE: Span = Span {
    first: 788,
    last: 797,
};
pub(super) const TABLE_S_ZIPG: Span = Span {
    first: 798,
    last: 799,
};
