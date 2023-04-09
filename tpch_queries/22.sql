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