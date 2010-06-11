import sqlalchemy as sa
import sqlalchemy.orm as orm
import sft_meta
import sft_schema 

def init_model(connection_endpoint):
    """ Call me before using any of the tables or classes in the model """
    
    engine = sa.create_engine(connection_endpoint)
    sft_meta.metadata.bind = engine
    sft_meta.metadata.create_all(checkfirst=True)

    sft_meta.engine = engine
    sft_meta.Session = orm.sessionmaker(bind=engine)



