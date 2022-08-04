from typing import *

class Usd:
    end: str
    val: int
    accn: str
    fy: int
    fp: str
    form: str
    filed: str


class Share(Usd):   
    frame: str


class UnitShares:
    shares: List[Share]


class UnitUsd:
    USD: List[Usd]


class EntityBase:
    label: str
    description: str


class EntityCommonStockSharesOutstanding(EntityBase):
    units: UnitShares


class EntityPublicFloat(EntityBase):
    units: UnitUsd


class Dei:
    EntityCommonStockSharesOutstanding: EntityCommonStockSharesOutstanding
    EntityPublicFloat: EntityPublicFloat


class Fact:
    dei: Dei
    us_gaap: Dict


class Company:
    cik: str
    entityName: str
    facts: Fact