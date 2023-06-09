/* Query 09 - Var_0 Rev_01 - TPC-H/TPC-R Product Type Profit Measure Query */
SELECT
        N_NAME  AS NATION,
        EXTRACT(YEAR FROM O_ORDERDATE) AS "YEAR",
        SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)-PS_SUPPLYCOST*L_QUANTITY (FLOAT)) (DECIMAL(18,2)) AS SUM_PROFIT
FROM
        PARTTBL,
        SUPPLIER,
        LINEITEM,
        PARTSUPP,
        ORDERTBL,
        NATION
WHERE
        S_SUPPKEY = L_SUPPKEY
        AND PS_SUPPKEY = L_SUPPKEY
        AND PS_PARTKEY = L_PARTKEY
        AND P_PARTKEY = L_PARTKEY
        AND O_ORDERKEY = L_ORDERKEY
        AND S_NATIONKEY = N_NATIONKEY
        AND P_NAME LIKE '%medium%'
GROUP BY
        NATION,
        "YEAR"
ORDER BY
        NATION,
        "YEAR" DESC;
