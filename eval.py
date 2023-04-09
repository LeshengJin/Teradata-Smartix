from database import Database

init_index = {
    "PARTTBL": ["P_PARTKEY"],
    "SUPPLIER": ["S_SUPPKEY", "S_NATIONKEY"],
    "PARTSUPP": ["PS_PARTKEY", "PS_SUPPKEY"],
    "CUSTOMER": ["C_CUSTKEY", "C_NATIONKEY"],
    "ORDERTBL": ["O_ORDERKEY", "O_CUSTKEY"],
    "LINEITEM": ["L_ORDERKEY", "L_LINENUMBER", "L_PARTKEY", "L_SUPPKEY"],
    "NATION": ["N_NATIONKEY", "N_REGIONKEY"],
    "REGION": ["R_REGIONKEY"],
}

mysql_index = {
    "PARTTBL": ["P_PARTKEY", "P_NAME", "P_CONTAINER"],
    "SUPPLIER": ["S_SUPPKEY", "S_NAME", "S_NATIONKEY", "S_COMMENT"],
    "PARTSUPP": ["PS_PARTKEY", "PS_AVAILQTY", "PS_SUPPLYCOST"],
    "CUSTOMER": ["C_NATIONKEY", "C_MKTSEGMENT", "C_COMMENT"],
    "ORDERTBL": [
        "O_ORDERKEY",
        "O_CUSTKEY",
        "O_ORDERPRIORITY",
        "O_SHIPPRIORITY",
        "O_COMMENT",
    ],
    "LINEITEM": [
        "L_ORDERKEY",
        "L_PARTKEY",
        "L_SUPPKEY",
        "L_LINENUMBER",
        "L_QUANTITY",
        "L_RETURNFLAG",
        "L_LINESTATUS",
        "L_SHIPDATE",
        "L_RECEIPTDATE",
    ],
    "NATION": ["N_COMMENT"],
    "REGION": ["R_REGIONKEY", "R_NAME"],
}


td_index = {
    "PARTTBL": ["P_PARTKEY", "P_MFGR", "P_SIZE", "P_COMMENT"],
    "SUPPLIER": ["S_SUPPKEY", "S_NATIONKEY", "S_NAME", "S_ADDRESS", "S_COMMENT"],
    "PARTSUPP": ["PS_PARTKEY", "PS_SUPPKEY", "PS_AVAILQTY", "PS_SUPPLYCOST"],
    "CUSTOMER": ["C_CUSTKEY", "C_NATIONKEY", "C_PHONE", "C_MKTSEGMENT"],
    "ORDERTBL": ["O_ORDERKEY", "O_CUSTKEY", "O_TOTALPRICE", "O_CLERK", "O_COMMENT"],
    "LINEITEM": ["L_ORDERKEY", "L_LINENUMBER", "L_PARTKEY", "L_SUPPKEY", "L_TAX"],
    "NATION": ["N_NATIONKEY", "N_REGIONKEY", "N_NAME"],
    "REGION": ["R_REGIONKEY", "R_NAME", "R_COMMENT"],
}

paper_index = {
    "PARTTBL": ["P_BRAND", "P_TYPE", "P_SIZE", "P_CONTAINER", "P_COMMENT"],
    "SUPPLIER": ["S_NAME", "S_ACCTBAL"],
    "PARTSUPP": ["PS_AVAILQTY", "PS_SUPPLYCOST", "PS_COMMENT"],
    "CUSTOMER": ["C_NAME", "C_ADDRESS", "C_PHONE", "C_MKTSEGMENT"],
    "ORDERTBL": [
        "O_ORDERSTATUS",
        "O_TOTALPRICE",
        "O_ORDERDATE",
        "O_CLERK",
        "O_SHIPPRIORITY",
    ],
    "LINEITEM": ["L_DISCOUNT", "L_TAX", "L_SHIPDATE", "L_COMMITDATE"],
    "NATION": ["N_COMMENT"],
    "REGION": ["R_COMMENT"],
}

db = Database()
db.tables = no_index

db.reset_indexes()

from TPCH import TPCH

tpch = TPCH()
tpch.run()
