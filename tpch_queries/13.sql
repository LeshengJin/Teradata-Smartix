/* Query 13 - Var_0 Rev_01 - TPC-H/TPC-R Customer Distribution Query */
SELECT
        C_COUNT, COUNT(*) AS CUSTDIST
FROM   (
        SELECT
                C_CUSTKEY,
                COUNT(O_ORDERKEY)
        FROM
                CUSTOMER LEFT OUTER JOIN ORDERTBL ON
                        C_CUSTKEY = O_CUSTKEY
                        AND O_COMMENT NOT LIKE '%express%accounts%'
        GROUP BY
                C_CUSTKEY
        ) AS C_ORDERS (C_CUSTKEY, C_COUNT)
GROUP BY
        C_COUNT
ORDER BY
        CUSTDIST DESC,
        C_COUNT DESC;