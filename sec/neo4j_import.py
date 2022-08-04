import zipfile
import os, sys
import json
from types import SimpleNamespace
from py2neo import Graph, Relationship, Node
from datetime import datetime
import numpy as np

sys.path.append('sec/databases')
sys.path.append('sec/models')

from databases.neo4j import get_connection, get_credentials
from models.company import *

"""
Leveraging data from the SEC, provide a list of:
* known company names
* aliases that can be searched in a unified way

This should include a:
* list of filings
* dates when they were filed
* and a user-friendly way of navigating through all of the filing data
"""

def create_graph_node(db:Graph, classname:str, name:str, props:Dict=None) -> Node:
    if props is None:
        node = Node(classname, name=name)
    else:
        node = Node(classname, name=name, **props)

    db.merge(node, classname, "name")
    return node

def get_filings(c: Company) -> List[Usd]:
    filings:List[Usd] = None

    if not hasattr(c, 'facts') or not hasattr(c.facts, 'dei'):
        return filings

    if hasattr(c.facts.dei, 'EntityPublicFloat'):
        units = c.facts.dei.EntityPublicFloat.units
        if hasattr(units, 'USD'):
            filings = units.USD

    elif hasattr(c.facts.dei, 'EntityCommonStockSharesOutstanding'):
        units = c.facts.dei.EntityCommonStockSharesOutstanding.units
        if hasattr(units, 'shares'):
            filings = units.shares

    return filings

def import_filing_year(db:Graph, yr:int) -> Node:
    node = create_graph_node(db, 'FY', str(yr), {'value': yr})
    return node

def import_filing_month(db:Graph, dt:datetime, yr_node: Node) -> Node:
    mo:int = dt.month
    mo_name:str = dt.strftime("%B")
    yr = yr_node['value']
    mo_node = create_graph_node(
        db=db,
        classname='Month', 
        name=f'{mo_name[0:3]}-{yr}', 
        props={'year': yr, 'month': mo, 'fullname': mo_name}
    )

    db.merge(Relationship.type('MONTH_OF_YEAR')(mo_node, yr_node), 'Year', 'name')
    return mo_node

def import_filing_day(db:Graph, dt:datetime, month_node: Node) -> Node:
    yr = month_node['year']
    mo = month_node['month']
    fullmonth = month_node['fullname']
    doy = dt.timetuple().tm_yday
    day = dt.day
    day_node = create_graph_node(
        db=db, 
        classname='Day',
        name=f'{fullmonth[0:3]} {day}, {yr}',
        props={
            'value': day,
            'doy': doy,
            'year': yr,
            'month': mo
        }
    )

    db.merge(Relationship.type('DAY_OF_MONTH')(day_node, month_node), 'Month', 'name')
    return day_node
    
def import_filing(db:Graph, filing:Usd, company_node: Node) -> Tuple[Node, Node, Node]:
    dt: datetime = datetime.strptime(filing.filed, '%Y-%m-%d')
    fy_node: Node = import_filing_year(db=db, yr=filing.fy)
    mo_node: Node = import_filing_month(db=db, dt=dt, yr_node=fy_node)
    day_node: Node = import_filing_day(db=db, dt=dt, month_node=mo_node)

    db.merge(Relationship.type('FILED_ON_DAY')(company_node, day_node), 'Company', 'name')
    db.merge(Relationship.type('FILED_ON_MONTH')(company_node, mo_node), 'Company', 'name')
    db.merge(Relationship.type('FILED_ON_YEAR')(company_node, fy_node), 'Company', 'name')

    return (fy_node, mo_node, day_node)

def import_company(company_json:bytes, db:Graph, trace=False) -> Node:
    """
    Parse SEC Company JSON
    """
    c: Company = json.loads(company_json, object_hook=lambda d: SimpleNamespace(**d))
    
    if not hasattr(c, 'entityName'):
        if trace:
            print('Company is missing entityName attribute.')
        return None

    if trace:
        print(f'Importing {c.entityName}')

    filings:List[Usd] = get_filings(c)
    
    company_props = {
        'cik': c.cik,
        'entityName': c.entityName
    }

    if filings is not None:
        evaluations = np.array([f.val for f in filings], dtype=int)
        company_props['mean_value'] = int(np.mean(evaluations))
        company_props['median_value'] = int(np.median(evaluations))
        company_props['min_value'] = int(np.min(evaluations))
        company_props['max_value'] = int(np.max(evaluations))
        company_props['current_value'] = int(evaluations[-1:])      

    company_node: Node = create_graph_node(db=db, classname='Company', name=c.entityName, props=company_props)
    
    if filings is not None:
        _ = list(map(lambda f: import_filing(db=db, filing=f, company_node=company_node), filings))

    return company_node
    

def read_zip_file(filepath:str, db:Graph, top:int=None, trace:bool=False) -> bool:
    """
    Read Zip File Without Extraction
    """
    i = 0
    success = 0
    with zipfile.ZipFile(filepath, 'r') as co:
        company_list = co.infolist()
        for fn in [f.filename for f in company_list][:top]:
            with co.open(fn, mode='r') as fb:
                company_node = import_company(fb.read(), db, trace)
                i += 1
                if company_node:
                    success += 1

    if trace:
        msg = f'{i} company imports completed.'
        if success < i:
            msg = f'{i-success} company imports out of {i} failed.'
        print(msg)

    return success == i


if __name__ == '__main__':
    creds = get_credentials().local
    db = get_connection(creds.ip, creds.password)
    read_zip_file(filepath=f'{os.getcwd()}/data/companyfacts.zip', db=db, trace=True, top=100)
    