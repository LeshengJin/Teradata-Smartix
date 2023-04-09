/* Query 19 - Var_0 Rev_1 - TPC-H/TPC-R Discounted Revenue Query */
SELECT
        SUM(L_EXTENDEDPRICE* (1 - L_DISCOUNT) (FLOAT)) (DECIMAL(18,2)) AS REVENUE
 FROM
        LINEITEM,
        PARTTBL
WHERE
        (
                P_PARTKEY = L_PARTKEY
                AND P_BRAND = 'Brand#23'
                AND P_CONTAINER IN  ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                AND L_QUANTITY >= 3 AND L_QUANTITY <= 3 + 10
                AND P_SIZE BETWEEN 1 AND 5
                AND L_SHIPMODE IN ('AIR', 'AIR REG')
                AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
        )
        OR
        (
                P_PARTKEY = L_PARTKEY
                AND P_BRAND = 'Brand#55'
                AND P_CONTAINER IN  ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                AND L_QUANTITY >= 14 AND L_QUANTITY <= 14 + 10
                AND P_SIZE BETWEEN 1  AND 10
                AND L_SHIPMODE IN ('AIR', 'AIR REG')
                AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
        )
        OR
        (
                P_PARTKEY = L_PARTKEY
                AND P_BRAND = 'Brand#21'
                AND P_CONTAINER IN  ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                AND L_QUANTITY >= 23 AND L_QUANTITY <= 23 + 10
                AND P_SIZE BETWEEN 1  AND 15
                AND L_SHIPMODE IN ('AIR', 'AIR REG')
                AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
        );