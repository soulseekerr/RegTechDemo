import numpy as np
import pandas as pd
import datetime

from api.databuilder import DataBuilder

class TradeBuilder(DataBuilder):
    """ Generate synthetic trade data and writes it to Parquet. """
    def __init__(self, cob_dt: datetime, run_id: str):
        self.books = np.array(["XVA","CVA","FVA","MM","EQD","FXD"])
        self.prods = np.array(["IRS","CDS","FXO","EQO","XCCY"])
        self.ccys  = np.array(["USD","EUR","GBP","JPY","AUD"])
        self.cps   = np.array(["A","B","C","D","E","F","G","H"])
        self.prodtypes = np.array(["Swap","Option","Cap/Floor","Forward","Future"])
        self.strattypes = np.array(["Curve","Basis","Vol","Dispersion","Carry","Roll"])
        self.strats = np.array(["DOFHOFS","DSCJOSF","DSFSDEE","ERTOOQW","FDSFNBV","FDSFJAA","FDSJDER"]) 
        self.start_date = "2024-01-01"
        self.maturity_date = "2025-01-01"
    
        def generator(cob_dt: datetime, n_rows: int, rng: np.random.Generator) -> pd.DataFrame:
            """Generates a chunk of n trades as a DataFrame."""
            return pd.DataFrame({
                "cob_dt": cob_dt.strftime("%Y%m%d"),
                "trade_id": np.arange(1, n_rows+1, dtype=np.int64),
                "fo_trade_id": np.arange(1, n_rows+1, dtype=np.int64),
                "book": rng.choice(self.books, n_rows),
                "product": rng.choice(self.prods, n_rows),
                "product_type": rng.choice(self.prodtypes, n_rows),
                "notional1": rng.lognormal(mean=13, sigma=1.0, size=n_rows).astype("float64"),
                "currency1": rng.choice(self.ccys, n_rows),
                "notional2": rng.lognormal(mean=13, sigma=1.0, size=n_rows).astype("float64"),
                "currency2": rng.choice(self.ccys, n_rows),
                "counterparty": rng.choice(self.cps, n_rows),
                "traded_at": pd.to_datetime(self.start_date) + pd.to_timedelta(rng.integers(0, 365, n_rows), unit="D"),
                "maturity": pd.to_datetime(self.maturity_date) + pd.to_timedelta(rng.integers(0, 365, n_rows), unit="D"),
                "strike": rng.uniform(0.8, 1.2, n_rows).astype("float64"),
                "rate": rng.uniform(0.01, 0.1, n_rows).astype("float64"),
                "quantity": rng.integers(1, 100, n_rows).astype("int32"),
                "strategy_type": rng.choice(self.strattypes, n_rows),
                "strategy": rng.choice(self.strats, n_rows),
                "is_fx": rng.choice([0,1], n_rows, p=[0.7,0.3]).astype("int8"),
                "is_commodity": rng.choice([0,1], n_rows, p=[0.9,0.1]).astype("int8"),
                "is_equity": rng.choice([0,1], n_rows, p=[0.85,0.15]).astype("int8")
            })
    
        super().__init__(cob_dt, run_id, "trades", generator)
