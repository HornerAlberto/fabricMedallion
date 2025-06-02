
import math
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, length
from pyspark.sql.types import StringType
from pyspark.sql.functions import max as pyspark_max

from typing import NoReturn, Callable




def script_as_select(df: DataFrame,
                           cols_map: Callable = (lambda x: x),
                           sql_style: bool = True,
                           optimize_floats: bool = False,
                           optimize_strings: bool = False,
                           printed=True
                           ) -> NoReturn:
    """
    Prints a select statement for the given DataFrame
    --------------------------------------------------------

    Parameters
    ----------
    df: pyspark.sql.DataFrame
        The DataFrame that is going to be scripted as 'Select to'.
    cols_map: Callable
        A mapping from one column_name to the intended column name.
    sql_style:
        Prints a SQL statement if True. Otherwise, prints a pyspark snippet.
    optimize_floats: bool
        converts floats to decimals.
    optimize_strings:
        Converts strings to varchar.

    Returns
    -------
    NoReturn
    """
    string_types_dict = dict()
    real_numbers_types_dict = dict()
    sql_types_dict = {
        "BinaryType": ("BINARY", ""),
        "BooleanType": ("BOOLEAN", ""),
        "ByteType": ("BYTE", ""),
        "DateType": ("DATE", ""),
        "DecimalType": ("DECIMAL(", ")"),
        "DoubleType": ("DOUBLE", ""),
        "FloatType": ("FLOAT", ""),
        "IntegerType": ("INT", ""),
        "TimestampType": ("TIMESTAMP", ""),
        "TimestampNTZType": ("TIMESTAMP_NTZ", ""),
        "LongType": ("BIGINT", ""),
        "ShortType": ("SMALLINT", ""),
        "StringType": ("STRING", ""),
        "MapType": ("STRING", ""), # Prevents errors for SQL statements
        "ArrayType": ("STRING", ""), # Prevents errors for SQL statements
        "VarcharType": ("VARCHAR(", ")"),
        "CharType": ("CHAR(", ")"),
    }
    new_cols = {col: cols_map(col) for col in df.columns}

    if (optimize_floats | optimize_strings):
        if optimize_strings:
            string_types_dict =             {
                    "StringType": ("VARCHAR(", ")"),
                    "VarcharType": ("VARCHAR(", ")"),
                    "CharType": ("VARCHAR(", ")"),
                }
            sql_types_dict.update(string_types_dict)
            len_df = df\
                .select(
                    *[pyspark_max(length(col(i).cast(StringType()))).alias(i)
                    for i in df.columns]
                )\
                .melt(
                    ids=[],
                    values=[i for i in df.columns],
                    variableColumnName="column_name",
                    valueColumnName="lenght"
                )
            len_dict = {
                row.column_name: row.lenght for row in len_df.collect()
            }
        if optimize_floats:
            real_numbers_types_dict = {
                    "DecimalType": ("DECIMAL(19, 8", ")"),
                    "DoubleType": ("DECIMAL(19, 8", ")"),
                    "FloatType": ("DECIMAL(19, 8", ")"),
                }
            sql_types_dict.update(real_numbers_types_dict)

        if not sql_style:
            raise NotImplementedError(
                "Data type optimization is"
                " only implemented for sql_style=True."
            )
        else:
            stmt = '"""\nSELECT \n\t'
            stmt_cols = list()
            for i in df.schema:
                data_type_class = str(i.dataType).split("(")[0]
                if (data_type_class in string_types_dict) and optimize_strings:
                    power = math.ceil(math.log(len_dict[i.name], 2))
                    if power <=9:
                        power+=2
                    params = str(2**(power))
                elif data_type_class in real_numbers_types_dict:
                    params = ""
                else:
                    params = str(i.dataType).split("(")[1][:-1]
                sql_type, ending = sql_types_dict[data_type_class]
                stmt_cols += [(
                    "CAST(" + i.name
                        + " AS "
                        + sql_type + params + ending
                        + ") AS "
                        + new_cols[i.name]
                    )]
            stmt+= "\n\t,".join(stmt_cols)
            stmt+= ("\n" r'FROM {df}' '\n""", df=df')

    else:
        if not sql_style:
            stmt = ".select(\n"
            for i in df.schema:
                stmt += (
                    "    col('" + i.name
                        + "').cast("
                        + str(i.dataType)
                        + ").alias('"
                        + new_cols[i.name]
                        +"'),"
                        + "\n"
                    )
            stmt+=")"
        else:
            stmt = '"""\nSELECT \n\t'
            stmt_cols = list()
            for i in df.schema:
                if str(i.dataType).split("(")[0] in {"ArrayType", "MapType"}:
                    params = ""
                else:
                    params = str(i.dataType).split("(")[1][:-1]
                sql_type, ending = sql_types_dict[
                    str(i.dataType).split("(")[0]
                ]
                stmt_cols += [(
                    "CAST(" + i.name
                        + " AS "
                        + sql_type + params + ending
                        + ") AS "
                        + new_cols[i.name]
                    )]
            stmt+= "\n\t,".join(stmt_cols)
            stmt+= ("\n" r'FROM {df}' '\n' r'""", df=df')
    if printed:
        print(stmt)
        return None
    else:
        return "df = spark.sql(\n" + stmt + "\n)"