import numpy as np
import pandas as pd
import datetime

from .databuilder import DataBuilder

class CptyBuilder(DataBuilder):
    """ Generate synthetic counterparty data and writes it to Parquet. """
    def __init__(self):
        self.countries = np.array(["FR","UK","NL","DE","JP","US","AU","CN","IN","BR"])
        self.cva_methods = np.array(["Financial","Corporate","Corporate_Exception", "Sovereign"])
        self.external_ratings = np.array(["AAA","AA","A","BBB","BB","B","CCC","CC","C","D"])
        self.grrs = np.array([0.15,0.25,0.50,0.65,0.75,0.80,0.85,0.90,0.95])
    
        def generator(cob_dt: datetime, n_rows: int, rng: np.random.Generator) -> pd.DataFrame:
            """Generates a chunk of n trades as a DataFrame."""
            return pd.DataFrame({
                "cob_dt": cob_dt.strftime("%Y%m%d"),
                "cpty_id": np.arange(1, n_rows+1, dtype=np.int64),
                "cpty_code": np.arange(1, n_rows+1, dtype=np.int64),
                "country": rng.choice(self.countries, n_rows),
                "cva_method": rng.choice(self.cva_methods, n_rows),
                "external_rating": rng.choice(self.external_ratings, n_rows),
                "grr": rng.choice(self.grrs, n_rows)
            })
    
        super().__init__("counterparties", generator)
