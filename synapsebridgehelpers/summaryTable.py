import synapsebridgehelpers
import synapseclient
import pandas as pd

DEFAULT_SUMMARY_COLUMNS = ['recordId', 'appVersion', 'phoneInfo', 'uploadDate', 'healthCode',
                           'externalId', 'dataGroups', 'createdOn', 'createdOnTimeZone',
                           'userSharingScope']


def summarizeTables(syn, projectId, columns=DEFAULT_SUMMARY_COLUMNS):
    """Summarizes all tables in a project by fetching the same columns from each table
    and concatenating the rows into a dataframe.

    Arguments:
    - syn: a Synapse client object
    - projectID: synapse ID of the project we want to summarize
    - columns: list of columns we want in the summary table.  If no columns
    are given, then all columns are used.
    """

    all_tables = synapsebridgehelpers.get_tables(syn, projectId)
    df_list = []
    for table_id in all_tables['id']:
        table_columns = [str(col.name) for col in syn.getTableColumns(table_id)]
        available_columns = list(set(columns).intersection(table_columns))
        columns_str = ','.join(available_columns)
        df = syn.tableQuery('select %s from %s' % (columns_str, table_id))
        df = df.asDataFrame()
        schema = syn.get(table_id)
        df['originalTableName'] = [schema.name for count in range(0, df.shape[0])]
        df['originalTableId'] = [schema.id for count in range(0, df.shape[0])]
        df_list.append(df)
    df_main = pd.concat(df_list)
    return df_main
