/* Query 17 - Var_0 Rev_01 - TPC-H/TPC-R Small-Quantity-Order Revenue Query */
SELECT
        SUM(L_EXTENDEDPRICE) / 7.0 AS AVG_YEARLY
FROM
        LINEITEM,
        PARTTBL
WHERE
        P_PARTKEY = L_PARTKEY
        AND P_BRAND = 'Brand#44'
        AND P_CONTAINER = 'SM CAN'
        AND L_QUANTITY < (
                SELECT
                        0.2 * AVG(L_QUANTITY)
                FROM
                        LINEITEM
                WHERE
                        L_PARTKEY = P_PARTKEY
        );