/* Query 12 - Var_0 Rev_01 - TPC-H/TPC-R Shipping Modes and Order Priority Query */
SELECT
        L_SHIPMODE,
        SUM(CASE
                WHEN O_ORDERPRIORITY = '1-URGENT'
                        OR O_ORDERPRIORITY = '2-HIGH'
                THEN 1
                ELSE 0
        END) AS HIGH_LINE_COUNT,
        SUM(CASE
                WHEN O_ORDERPRIORITY <> '1-URGENT'
                        AND O_ORDERPRIORITY <> '2-HIGH'
                THEN 1
                ELSE 0
        END) AS LOW_LINE_COUNT
FROM
        ORDERTBL,
        LINEITEM
WHERE
        O_ORDERKEY = L_ORDERKEY
        AND L_SHIPMODE IN ('REG AIR', 'TRUCK')
        AND L_COMMITDATE < L_RECEIPTDATE
        AND L_SHIPDATE < L_COMMITDATE
        AND L_RECEIPTDATE >= '1996-01-01'
        AND L_RECEIPTDATE < DATE '1996-01-01' + INTERVAL '1' YEAR
GROUP BY
        L_SHIPMODE
ORDER BY
        L_SHIPMODE;