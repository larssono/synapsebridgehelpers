import pandas as pd
from  multiprocessing.dummy import Pool 

def get_tables(syn, projectId):
    """Returns all the tables in a projects as a dataFrame with 
    columns for synapseId, table names and simplified Name."""
    
    tables = syn.chunkedQuery('select name,id from table where projectId=="%s"' %projectId)
    tables = pd.DataFrame(list(tables))
    tables = tables[(tables['table.name']!='parkinson-status') &(tables['table.name']!='parkinson-appVersion')]
    names = [name.replace('parkinson-', '').replace('Parkinsons-', '').replace('Activity-', '')
             for name in tables['table.name']]
    tables['simpleName']=names
    return tables 

def find_tables_with_data(syn, tables, healthCodes):
    """Go through a list of tables and find those where there is data given a
    data frame with sought healthCodes.

    Returns the tables data frame with an additional column containing the 
    number of unique healthCodes found in each table."""
    query = ("select count(distinct healthCode) from %s where healthCode in ('"+
            "','".join(healthCodes)+"')")
    counts = [syn.tableQuery(query %synId, resultsAs='rowset').asInteger() for
              synId in tables['table.id']]
    tables['healthCodeCounts'] = counts
    return tables

def query_across_tables(syn, tables, query):
    """Runs a query across a list of tables and returns a list of results
    
    param syn: a synapse object obtained through a syn.login()
    param tables: a list of synapse ids
    param query: a query with a unassigne string in the from clause. E.g. "select foo from %s" 

    """
    mp = Pool(8)
    return mp.map(lambda synId: syn.tableQuery(query %synId), tables)
