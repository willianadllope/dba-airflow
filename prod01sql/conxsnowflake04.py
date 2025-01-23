from sqlalchemy import create_engine

engine = create_engine(
    'snowflake://{user}:{password}@{account_identifier}/'.format(
        user='willian',
        password='BillPoker13',
        account_identifier='ZK69750',
    )
)
try:
    connection = engine.connect()
    results = connection.execute('select current_version()').fetchone()
    print(results[0])
    connection.close()
finally:
    engine.dispose()