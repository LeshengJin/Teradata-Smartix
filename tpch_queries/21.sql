/* Query 21 - Var_0 Rev_01 - TPC-H/TPC-R  The Suppliers Who Kept Orders Waiting Query */
SELECT
        S_NAME,
        COUNT(*) AS NUMWAIT
FROM
        SUPPLIER,
        LINEITEM L1,
        ORDERTBL,
        NATION
WHERE
        S_SUPPKEY = L1.L_SUPPKEY
        AND O_ORDERKEY = L1.L_ORDERKEY
        AND O_ORDERSTATUS='F'
        AND L1.L_RECEIPTDATE > L1.L_COMMITDATE
        AND EXISTS (
                SELECT
                        *
                FROM
                        LINEITEM L2
                WHERE
                        L2.L_ORDERKEY = L1.L_ORDERKEY
                        AND L2.L_SUPPKEY <> L1.L_SUPPKEY
        )
        AND NOT EXISTS (
                SELECT
                        *
                FROM
                        LINEITEM L3
                WHERE
                        L3.L_ORDERKEY = L1.L_ORDERKEY
                        AND L3.L_SUPPKEY <> L1.L_SUPPKEY
                        AND L3.L_RECEIPTDATE > L3.L_COMMITDATE
        )
        AND S_NATIONKEY = N_NATIONKEY
        AND N_NAME = 'INDONESIA'
GROUP BY
        S_NAME
ORDER BY
        NUMWAIT DESC,
        S_NAME;