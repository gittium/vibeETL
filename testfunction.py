from sqlalchemy import create_engine , inspect

DATABASE_URL = (
    "postgresql+psycopg2://postgres:admin@localhost:5432/postgres"
)
engine     = create_engine(DATABASE_URL, pool_pre_ping=True)
inspector  = inspect(engine)

# def get_schema(): #-> Dict[str, List[str]]
#     """Return {table: [column, …]} for front-end picker."""
#     tables = inspector.get_table_names()
#     table_dict = {}
#     for table in tables:
        
#         columns = inspector.get_columns(table)
#         table_columns = [col['name'] for col in columns]
#         table_dict[table] = table_columns
#     return table_dict

# print((get_schema()))
