from use_case.helpers.base import Base
from use_case.helpers.base_ingest import BaseIngest

class F(Base, BaseIngest):
    def __init__(self):
        print("f")
