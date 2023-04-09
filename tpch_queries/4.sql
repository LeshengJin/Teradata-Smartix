/* Query 04 - Var_0 Rev_01 - TPC-H/TPC-R Order Priority Checking Query */
SELECT
        O_ORDERPRIORITY,
        COUNT(*) AS ORDER_COUNT
FROM
        ORDERTBL
WHERE
        O_ORDERDATE >=  '1993-11-01'
        AND O_ORDERDATE < DATE '1993-11-01' + INTERVAL '3' MONTH
        AND EXISTS (
                SELECT
                        *
                FROM
                        LINEITEM
                WHERE
                        L_ORDERKEY = O_ORDERKEY
                        AND L_COMMITDATE < L_RECEIPTDATE
        )
GROUP BY
        O_ORDERPRIORITY
ORDER BY
        O_ORDERPRIORITY;