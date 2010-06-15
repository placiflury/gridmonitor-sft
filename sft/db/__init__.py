import sqlalchemy as sa
import sqlalchemy.orm as orm
import sft_meta
import sft_schema 

def init_model(engine):
    """ Call me before using any of the tables or classes in the model """
    
    sft_meta.engine = engine
    sft_meta.metadata.bind = engine
    sft_meta.metadata.create_all(checkfirst=True)
    
    ses=orm.sessionmaker(bind=engine)
    sft_meta.Session = orm.scoped_session(ses)

