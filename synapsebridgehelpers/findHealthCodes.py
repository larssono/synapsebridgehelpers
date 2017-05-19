import pandas as pd

def externalIds2healthCodes(syn,tables):
    """Given a list of tables determines 
    the healthCodes that map to externalIds.
    
    This function is used to find healthCodes in case that data is 
    contributed both before and after externalIds have been assigned.
    
    Arguments:
    - `syn`: a Synapse client object
    - `tables`: list of table Ids
    """
    QUERY = 'SELECT distinct externalId, healthCode FROM %s'
    dfs = []
    for table in tables:
        idMap = syn.tableQuery(QUERY %table).asDataFrame()
        dfs.append(idMap)
    idMap = pd.concat(dfs)
    return idMap.drop_duplicates().dropna()
    
    
