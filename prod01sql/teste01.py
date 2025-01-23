import pandas as pd
import urllib as ul
import fastparquet as fp
import sqlalchemy as sa

def main():
    sqlInstance = 'prod01sql.systax.com.br'
    database = 'systax_app'
    tableName = 'clientes_rds'
    props = ul.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                    "SERVER=" + sqlInstance + ";"
                                    "DATABASE=" + database + ";"
                                    "Trusted_Connection=yes;")
    con = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(props))
    fetch_batch_size = 1000
    metadata = sa.schema.MetaData(bind=con)
    table = sa.Table(tableName, metadata, autoload=True)

    # Generate pandas/python compatible datatype mapping
    map = {}
    data_type_map_lookup = {
        'int64': ['smallint', 'tinyint', 'integer'],
        'float': ['bigint', 'float', 'real'],
        'str': ['char', 'nchar', 'nvarchar', 'nvarchar(max)', 'uniqueidentifier', 'varchar(n)', 'varchar(max)'],
        'datetime64[ns]': ['date', 'datetime', 'smalldatetime'],
        'bytes': ['binary', 'varbinary', 'varbinary(max)'],
        'bool': ['bit']
    }
    for col in table.columns:
        for key, value in data_type_map_lookup.items():
            types = data_type_map_lookup[key]
            if list(filter(str(col.type).lower().startswith, types)):
                if col.nullable and key == 'int64':
                    map[col.name] = 'float'
                else:
                    map[col.name] = key

    #Fetch data
    output = table.select().execution_options(stream_results=True).execute()
    append = False
    while True:
        batch = output.fetchmany(fetch_batch_size)
        if not len(batch) > 0:
            break
        else:
            df = (pd.DataFrame(data=batch, columns=map)).astype(dtype=map)
            print(df.to_string())  # Prints good
            fp.write("C:\\python\\test01.parquet", df, write_index=False, compression=False, append=append)
        append = True


if __name__ == "__main__":
    main()