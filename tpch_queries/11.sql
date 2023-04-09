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