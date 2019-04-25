import synapseclient
import synapsebridgehelpers
import pandas as pd


def transferTables(syn, sourceProjId, uploadProjId, extId_Str='',
                   simpleNameFilters=[], healthCodeList=None):
    """ This function transfers tables from a source project to the upload
    project (target project) sorted by external Ids which contain extId_Str,
    group tables with simpleNameFilters, also can filter
    tables by healthcodes and then group by activity"""

    # dataframe of all tables using get_tables from synapsebridgehelper.tableHelpers
    all_tables = synapsebridgehelpers.get_tables(syn, sourceProjId,
                                                 simpleNameFilters)
    print(all_tables)
    # Converting externalIds to healthCodes
    if extId_Str != '':
        res = synapsebridgehelpers.externalIds2healthCodes(syn, list(all_tables['table.id']))
        res = res[res['externalId'].str.contains(extId_Str)]
        healthCodeList = list(res['healthCode'])

    # List of tables sorted by activity and filtered using healthcodes
    tables_dict = synapsebridgehelpers.filterTablesByActivity(syn, all_tables,
                                                              healthCodes=healthCodeList)

    # Iterate over each activity in tables_dict
    for activity_, activityTableIds in tables_dict.items():
        df_list = []                  # list of dataframes corresponding to that activity
        cols_filehandleid = []        # list of columns that have type FILEHANDLEID

        # looping over all tables corresponding to that activity
        for table_index in range(0, len(activityTableIds)):
            result = synapsebridgehelpers.tableWithFileIds(syn,
                                                           table_id=activityTableIds[table_index],
                                                           healthcodes=healthCodeList)
            cols_filehandleid = (cols_filehandleid +
                                 list(set(result['cols']) - set(cols_filehandleid)))
            df_list.append(result['df'])

        # Concatenating all tables to form one table for the activity
        df_main = pd.concat(df_list)
        cols = synapseclient.as_table_columns(df_main)

        # Change the type of columns that are FILEHANDLEIDs as calculated before
        for col in cols:
            if col.name in cols_filehandleid:
                col.columnType = 'FILEHANDLEID'

        # If different datatypes happen while merging tables this will change
        # the column type in the resulting dataframe
        # The following code sets it right and casts the data into its original
        # form / form that syn.store would accept
        # (for FILEHANDLEID type columns, the input needs to be an integer)
        for col in cols:
            if col.columnType == 'STRING':
                df_main[col.name] = [str(item) if item == item else '' for item in df_main[col.name]]
            elif col.columnType == 'INTEGER':
                df_main[col.name] = [int(item) if item == item else '' for item in df_main[col.name]]
            elif col.columnType == 'FILEHANDLEID':
                df_main[col.name] = [int(item) if (item != '' and item == item) else ''
                                     for item in df_main[col.name]]
            else:
                df_main[col.name] = [item if item == item else '' for item in df_main[col.name]]

        # Updaing schema and uploading
        schema = synapseclient.Schema(name=activity_, columns=cols, parent=uploadProjId)
        table = synapseclient.Table(schema, df_main)
        table = syn.store(table)
        table = syn.setProvenance(table. schema.id,
                                  activity=synapseclient.activity.Activity(used=tables_dict[activity_]))
