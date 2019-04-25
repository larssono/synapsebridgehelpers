import pandas as pd
from multiprocessing.dummy import Pool
from synapseclient.exceptions import SynapseHTTPError


def get_tables(syn, projectId, simpleNameFilters=[]):
    """Returns all the tables in a projects as a dataFrame with
    columns for synapseId, table names, Version and Simplified Name

     Arguments:
    - syn: a Synapse client object
    - projectId: Synapse ID of the project we want tables from
    - simpleNameFilters: the strings that are to be
    filtered out from the table names to create a simple name"""

    tables = syn.getChildren(projectId, includeTypes=['table'])
    tables = pd.DataFrame(list(tables))
    # removing tables named 'parkinson-status' and 'parkinson-appVersion'
    tables = tables[(tables['name'] != 'parkinson-status') &
                    (tables['name'] != 'parkinson-appVersion')]
    tables['version'] = tables['name'].str.extract('(.)(-v\d+)', expand=True)[1]
    names = tables['name'].str.extract('([ -_a-z-A-Z\d]+)(-v\d+)', expand=True)[0]
    print(names)
    for word in simpleNameFilters:
        names = [name.replace(word, '') for name in names]
    tables['simpleName'] = names
    return tables


def find_tables_with_data(syn, tables, healthCodes):
    """Go through a list of tables and find those where there is data given a
    data frame with sought healthCodes.

    Returns the tables data frame with an additional column containing the
    number of unique healthCodes found in each table."""
    query = ("select count(distinct healthCode) from %s where healthCode in ('" +
             "','".join(healthCodes) + "')")
    counts = []
    for synId in tables['id']:
        try:
            n = syn.tableQuery(query % synId, resultsAs='rowset').asInteger()
        except SynapseHTTPError as err:  # Catch eror where healthCode not in table
            if err.response.status_code == 400:
                n = 0
            else:
                raise err
        counts.append(n)
    tables['healthCodeCounts'] = counts
    return tables


def query_across_tables(syn, tables, query, continueOnMissingColumn=True):
    """Runs a query across a list of tables and returns a list of results

    param syn: a synapse object obtained through a syn.login()
    param tables: a list of synapse ids
    param query: a query with a unassigne string in the from clause. E.g. "select foo from %s"

    """
    def safeQuery(query):
        try:
            return syn.tableQuery(query)
        except SynapseHTTPError as err:
            if err.response.status_code == 400 and continueOnMissingColumn:
                return
            else:
                raise err

    mp = Pool(8)
    return mp.map(lambda synId: safeQuery(query % synId), tables)
