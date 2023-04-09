/* Query 18 - Var_0 Rev_01 - TPC-H/TPC-R Large Volume Customer Query */
/* Return the first 100 selected rows                                */
SELECT
        C_NAME,
        C_CUSTKEY,
        O_ORDERKEY,
        O_ORDERDATE,
        O_TOTALPRICE,
        SUM(L_QUANTITY (FLOAT)) (DECIMAL(18,2)) AS SUM_QTY
FROM
        CUSTOMER,
        ORDERTBL,
        LINEITEM
WHERE
        O_ORDERKEY IN (
                SELECT
                        L_ORDERKEY
                FROM
                        LINEITEM
                GROUP BY
                        L_ORDERKEY HAVING
                                SUM(L_QUANTITY) > 312
        )
        AND C_CUSTKEY = O_CUSTKEY
        AND O_ORDERKEY = L_ORDERKEY
GROUP BY
        C_NAME,
        C_CUSTKEY,
        O_ORDERKEY,
        O_ORDERDATE,
        O_TOTALPRICE
ORDER BY
        O_TOTALPRICE DESC,
        O_ORDERDATE;