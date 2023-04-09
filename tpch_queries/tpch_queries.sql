.logon dbc/dbc,dbc

DATABASE TPCH;

/* Query 01 - Var_0 Rev_01 - TPC-H/TPC-R Pricing Summary Report Query   */
SELECT
        L_RETURNFLAG,
        L_LINESTATUS,
        SUM(L_QUANTITY (FLOAT)) (DECIMAL(18,2)) AS SUM_QTY,
        SUM(L_EXTENDEDPRICE (FLOAT)) (DECIMAL(18,2)) AS SUM_BASE_PRICE,
        SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT) (FLOAT)) (DECIMAL(18,2)) AS SUM_DISC_PRICE,
        SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)*(1+L_TAX) (FLOAT)) (DECIMAL(18,2)) AS SUM_CHARGE,
        AVG(L_QUANTITY (FLOAT)) (DECIMAL(18,2)) AS AVG_QTY,
        AVG(L_EXTENDEDPRICE (FLOAT)) (DECIMAL(18,2)) AS AVG_PRICE,
        AVG(L_DISCOUNT (FLOAT)) (DECIMAL(18,2)) AS AVG_DISC,
        COUNT(*) (DEC(18,0)) AS COUNT_ORDER
FROM
        LINEITEM
WHERE
        L_SHIPDATE <= DATE '1998-12-01' - INTERVAL '77' DAY
GROUP BY
        L_RETURNFLAG,
        L_LINESTATUS
ORDER BY
        L_RETURNFLAG,
        L_LINESTATUS;

/* Query 02 - Var_0 Rev_01 - TPC-H/TPC-R Minimum Cost Supplier Query  */
SELECT
        S_ACCTBAL,
        S_NAME,
        N_NAME,
        P_PARTKEY,
        P_MFGR,
        S_ADDRESS,
        S_PHONE,
        S_COMMENT
FROM
        PARTTBL,
        SUPPLIER,
        PARTSUPP,
        NATION,
        REGION
WHERE
        P_PARTKEY = PS_PARTKEY
        AND S_SUPPKEY = PS_SUPPKEY
        AND P_SIZE = 11
        AND P_TYPE LIKE '%TIN'
        AND S_NATIONKEY = N_NATIONKEY
        AND N_REGIONKEY = R_REGIONKEY
        AND R_NAME = 'AMERICA'
        AND PS_SUPPLYCOST = (
                SELECT
                        MIN(PS_SUPPLYCOST)
                FROM
                        PARTSUPP,
                        SUPPLIER,
                        NATION,
                        REGION
                WHERE
                        P_PARTKEY = PS_PARTKEY
                        AND S_SUPPKEY = PS_SUPPKEY
                        AND S_NATIONKEY = N_NATIONKEY
                        AND N_REGIONKEY = R_REGIONKEY
                        AND R_NAME = 'AMERICA'
        )
ORDER BY
        S_ACCTBAL DESC,
        N_NAME,
        S_NAME,
        P_PARTKEY;

/* Query 03 - Var_0 Rev_01 - TPC-H/TPC-R Shipping Priority Query      */
SELECT
        L_ORDERKEY,
        SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT) (FLOAT)) (DECIMAL(18,2)) AS REVENUE,
        O_ORDERDATE,
        O_SHIPPRIORITY
FROM
        CUSTOMER,
        ORDERTBL,
        LINEITEM
WHERE
        C_MKTSEGMENT  = 'FURNITURE'
        AND C_CUSTKEY    = O_CUSTKEY
        AND L_ORDERKEY   = O_ORDERKEY
        AND O_ORDERDATE  < '1995-03-16'
        AND L_SHIPDATE   > '1995-03-16'
GROUP BY
        L_ORDERKEY,
        O_ORDERDATE,
        O_SHIPPRIORITY
ORDER BY
        REVENUE DESC,
        O_ORDERDATE;

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

/* Query 05 - Var_0 Rev_01 - TPC-H/TPC-R Local Supplier Volume Query */
SELECT
        N_NAME,
        SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT) (FLOAT)) (DECIMAL(18,2)) AS REVENUE
FROM
        CUSTOMER,
        ORDERTBL,
        LINEITEM,
        SUPPLIER,
        NATION,
        REGION
WHERE
        C_CUSTKEY = O_CUSTKEY
        AND L_ORDERKEY = O_ORDERKEY
        AND L_SUPPKEY = S_SUPPKEY
        AND C_NATIONKEY = S_NATIONKEY
        AND S_NATIONKEY = N_NATIONKEY
        AND N_REGIONKEY = R_REGIONKEY
        AND R_NAME = 'ASIA'
        AND O_ORDERDATE >= '1993-01-01'
        AND O_ORDERDATE < DATE '1993-01-01' + INTERVAL '1' YEAR
GROUP BY
        N_NAME
ORDER BY
        REVENUE DESC;

/* Query 06 - Var_0 Rev_01 - TPC-H/TPC-R Forecasting Revenue Change Query */
SELECT
        SUM(L_EXTENDEDPRICE*L_DISCOUNT (FLOAT)) (DECIMAL(18,2)) AS REVENUE
FROM
        LINEITEM
WHERE
        L_SHIPDATE >= '1995-01-01'
        AND L_SHIPDATE < DATE '1995-01-01' + INTERVAL '1' YEAR
        AND L_DISCOUNT BETWEEN 0.04 - 0.01 AND 0.04 + 0.01
        AND L_QUANTITY < 25;

/* Query 07 - Var_0 Rev_01 - TPC-H/TPC-R Volume Shipping Query  */
SELECT
        N1.N_NAME  AS SUPP_NATION,
        N2.N_NAME  AS CUST_NATION,
        EXTRACT(YEAR FROM L_SHIPDATE) AS "YEAR",
        SUM(L_EXTENDEDPRICE * (1-L_DISCOUNT) (FLOAT)) (DECIMAL(18,2)) AS REVENUE
FROM
        SUPPLIER,
        LINEITEM,
        ORDERTBL,
        CUSTOMER,
        NATION N1,
        NATION N2
WHERE
        S_SUPPKEY  = L_SUPPKEY
        AND O_ORDERKEY = L_ORDERKEY
        AND C_CUSTKEY = O_CUSTKEY
        AND S_NATIONKEY = N1.N_NATIONKEY
        AND C_NATIONKEY = N2.N_NATIONKEY
        AND (
                (N1.N_NAME = 'MOZAMBIQUE' AND N2.N_NAME = 'KENYA')
                OR (N1.N_NAME = 'KENYA' AND N2.N_NAME = 'MOZAMBIQUE')
        )
        AND  L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
GROUP BY
        SUPP_NATION,
        CUST_NATION,
        "YEAR"
ORDER BY
        SUPP_NATION,
        CUST_NATION,
        "YEAR";

/* Query 08 - Var_0 Rev_01 - TPC-H/TPC-R National Market Share Query */
SELECT
        EXTRACT(YEAR FROM O_ORDERDATE) AS "YEAR",
        SUM(CASE
                WHEN N2.N_NAME = 'UNITED STATES'
                THEN (L_EXTENDEDPRICE*(1-L_DISCOUNT) (FLOAT))
                ELSE 0
        END) / SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT) (FLOAT)) (DECIMAL(18,2)) AS MKT_SHARE
FROM
        PARTTBL,
        SUPPLIER,
        LINEITEM,
        ORDERTBL,
        CUSTOMER,
        NATION N1,
        NATION N2,
        REGION
WHERE
        P_PARTKEY = L_PARTKEY
        AND S_SUPPKEY = L_SUPPKEY
        AND L_ORDERKEY = O_ORDERKEY
        AND O_CUSTKEY = C_CUSTKEY
        AND C_NATIONKEY = N1.N_NATIONKEY
        AND N1.N_REGIONKEY = R_REGIONKEY
        AND R_NAME = 'AMERICA'
        AND S_NATIONKEY = N2.N_NATIONKEY
        AND O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
        AND P_TYPE = 'SMALL ANODIZED TIN'
GROUP BY
        "YEAR"
ORDER BY
        "YEAR";

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

/* Query 10 - Var_0 Rev_01 - TPC-H/TPC-R Returned Item Reporting Query */
SELECT
        C_CUSTKEY,
        C_NAME,
        SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT) (FLOAT)) (DECIMAL(18,2)) AS REVENUE,
        C_ACCTBAL,
        N_NAME,
        C_ADDRESS,
        C_PHONE,
        C_COMMENT
FROM
        CUSTOMER,
        ORDERTBL,
        LINEITEM,
        NATION
WHERE
        C_CUSTKEY = O_CUSTKEY
        AND L_ORDERKEY = O_ORDERKEY
        AND O_ORDERDATE >= '1993-08-01'
        AND O_ORDERDATE < DATE '1993-08-01' + INTERVAL '3' MONTH
        AND L_RETURNFLAG = 'R'
        AND C_NATIONKEY = N_NATIONKEY
GROUP BY
        C_CUSTKEY,
        C_NAME,
        C_ACCTBAL,
        C_PHONE,
        N_NAME,
        C_ADDRESS,
        C_COMMENT
ORDER BY
        REVENUE DESC;

/* Query 11 - Var_0 Rev_01 - TPC-H/TPC-R Important Stock Identification Query */
SELECT
        PS_PARTKEY,
        SUM(PS_SUPPLYCOST * PS_AVAILQTY (FLOAT)) (DEC(18,2)) AS "VALUE"
FROM
        PARTSUPP,
        SUPPLIER,
        NATION
WHERE
        PS_SUPPKEY = S_SUPPKEY
        AND S_NATIONKEY = N_NATIONKEY
        AND N_NAME = 'VIETNAM'
GROUP BY
        PS_PARTKEY HAVING
                SUM(PS_SUPPLYCOST * PS_AVAILQTY) > (
                        SELECT
                                SUM(PS_SUPPLYCOST * PS_AVAILQTY (FLOAT)) * 0.000050
                        FROM
                                PARTSUPP,
                                SUPPLIER,
                                NATION
                        WHERE
                                PS_SUPPKEY = S_SUPPKEY
                                AND S_NATIONKEY = N_NATIONKEY
                                AND N_NAME = 'VIETNAM'
                )
ORDER BY
        "VALUE" DESC;

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

/* Query 14 - Var_0 Rev_01 - TPC-H/TPC-R Promotion Effect Query */
SELECT
        100.00*SUM(CASE
                WHEN P_TYPE LIKE 'PROMO%'
                THEN L_EXTENDEDPRICE*(1-L_DISCOUNT)
                ELSE 0
        END (FLOAT)) / SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT) (FLOAT)) (DECIMAL (18,2)) AS PROMO_REVENUE
FROM
        LINEITEM,
        PARTTBL
WHERE
        L_PARTKEY = P_PARTKEY
        AND L_SHIPDATE >= '1995-03-01'
        AND L_SHIPDATE < DATE '1995-03-01' + INTERVAL '1' MONTH;

/* QUERY 15 - VAR_0 REV_01 - TPC-H/TPC-R  TOP SUPPLIER QUERY */
WITH REVENUE(SUPPLIER_NO, TOTAL_REVENUE) AS
        (
                SELECT L_SUPPKEY, SUM(L_EXTENDEDPRICE * (1-L_DISCOUNT) (FLOAT)) (DECIMAL(18,2))
                FROM LINEITEM
                WHERE L_SHIPDATE >= DATE '1995-04-01' AND L_SHIPDATE < DATE '1995-04-01'+ INTERVAL '3' MONTH
                GROUP BY L_SUPPKEY
        )
SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE, TOTAL_REVENUE
FROM SUPPLIER, REVENUE
WHERE S_SUPPKEY = SUPPLIER_NO AND TOTAL_REVENUE = (SELECT MAX(TOTAL_REVENUE) FROM REVENUE)
ORDER BY S_SUPPKEY;

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

/* Query 20 - Var_0 Rev_01 - TPC-H/TPC-R The Potential Part Promotion query */
SELECT
        S_NAME,
        S_ADDRESS
FROM
        SUPPLIER,
        NATION
WHERE
        S_SUPPKEY IN (
                SELECT
                        PS_SUPPKEY
                FROM
                        PARTSUPP
                WHERE
                        PS_PARTKEY IN (
                                SELECT
                                        P_PARTKEY
                                FROM
                                        PARTTBL
                                WHERE
                                        P_NAME LIKE 'moccasin%'
                        )
                AND  PS_AVAILQTY > (
                        SELECT
                                0.5 * SUM(L_QUANTITY)
                        FROM
                                LINEITEM
                        WHERE
                                L_PARTKEY = PS_PARTKEY
                                AND  L_SUPPKEY = PS_SUPPKEY
                                AND  L_SHIPDATE >= '1993-01-01'
                                AND  L_SHIPDATE <  DATE '1993-01-01' + INTERVAL '1' YEAR
                )
        )
        AND S_NATIONKEY = N_NATIONKEY
        AND N_NAME = 'JAPAN'
ORDER BY
        S_NAME;

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

/* Query 22 - Var_0 Rev_01 - TPC-H/TPC-R Global Saleds opportunity Query */
SELECT
        CNTRYCODE,
        COUNT(*) AS NUMCUST,
        SUM(C_ACCTBAL) AS TOTACCTBAL
FROM    (
        SELECT
                SUBSTRING (C_PHONE FROM 1 FOR 2) AS CNTRYCODE,
                C_ACCTBAL
        FROM
                CUSTOMER
        WHERE
                SUBSTRING (C_PHONE FROM 1 FOR 2) IN
                        ('27', '24', '17', '22', '33', '32', '11', '') 
                AND C_ACCTBAL > (
                        SELECT
                                AVG(C_ACCTBAL)
                        FROM
                                CUSTOMER
                        WHERE
                                C_ACCTBAL > 0.00
                                AND SUBSTRING (C_PHONE FROM 1 FOR 2) IN
					('27', '24', '17', '22', '33', '32', '11')
                )
                AND NOT EXISTS (
                        SELECT
                                *
                        FROM
                                ORDERTBL
                        WHERE
                                O_CUSTKEY=C_CUSTKEY
                )
        ) AS CUSTSALE
GROUP BY
        CNTRYCODE
ORDER BY
        CNTRYCODE;


.logoff
.quit
