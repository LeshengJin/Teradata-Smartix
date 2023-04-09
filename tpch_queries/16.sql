/* Query 16 - Var_0 Rev_01 - TPC-H/TPC-R Parts/Supplier Relationship Query */
SELECT
        P_BRAND,
        P_TYPE,
        P_SIZE,
        COUNT(DISTINCT PS_SUPPKEY) AS SUPPLIER_CNT
FROM
        PARTSUPP,
        PARTTBL
WHERE
        P_PARTKEY = PS_PARTKEY
        AND P_BRAND <> 'Brand#24'
        AND P_TYPE NOT LIKE 'ECONOMY POLISHED%'
        AND P_SIZE IN (7, 17, 15, 48, 21, 1, 44, 11) 
        AND PS_SUPPKEY NOT IN (
                SELECT
                        S_SUPPKEY
                FROM
                        SUPPLIER
                WHERE
                        S_COMMENT LIKE '%Customer%Complaints%'
        )
GROUP BY
        P_BRAND,
        P_TYPE,
        P_SIZE
ORDER BY
        SUPPLIER_CNT DESC,
        P_BRAND,
        P_TYPE,
        P_SIZE;