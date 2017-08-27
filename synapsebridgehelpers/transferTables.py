import synapseclient
import synapsebridgehelpers
import numpy as np
import pandas as pd

def transferTables(syn,sourceProjId, uploadProjId, extId_Str = '', simpleNameFilters =[], healthCodeList=None):
    """ This function transfers tables from a source project to the upload project (target project) 
    sorted by external Ids which contain extId_Str, group tables with simpleNameFilters, also can filter
    tables by healthcodes and then group by activity"""

    # List of tables sorted by activity as defined in groupTableActivity, which is based on get_tables from
    # synapsebridgehelper.tableHelpers
    all_tables = synapsebridgehelpers.groupTableActivity(syn,sourceProjId, activityNameFilters = simpleNameFilters)
    if extId_Str != '':
        all_tables_list = []
        for activity in all_tables:
            all_tables_list = all_tables_list + all_tables[activity]
        res = synapsebridgehelpers.externalIds2healthCodes(syn,all_tables_list)
        res = res[res['externalId'].str.contains(extId_Str)]
        healthCodeList = list(res['healthCode'])
        extId_Str = ''

    tables_list = synapsebridgehelpers.groupTableActivity(syn, sourceProjId, activityNameFilters = simpleNameFilters, healthCodes = healthCodeList)        
    
    # Iterate over each activity in tables_list
    for activity_ in tables_list:
        print(activity_)
        
        # list of all table ids corresponding to that activity 
        activityTableIds = tables_list[activity_]
        result = synapsebridgehelpers.tableWithFileIds(syn,table_id = activityTableIds[0], healthcodes = healthCodeList)
        df_main = result['df']
        cols = result['cols']
        
        # appending the rest of the sorted tables corresponding to that activity if they exist
        for table_index in range(1, len(activityTableIds)):
            result = synapsebridgehelpers.tableWithFileIds(syn,table_id = activityTableIds[table_index], healthcodes = healthCodeList)
            df = result['df']
            cols = result['cols']
            df_main = pd.concat([df_main, df])

        # If different datatypes happen while merging tables this will change the column type in the resulting dataframe
        # The following code sets it right and casts the data into its original form / form that syn.store would accept
        # (for FILEHANDLEID type columns, the input needs to be an integer)
        for col in cols:
            if col.columnType == 'STRING': 
                df_main[col.name] = [str(item) if item==item else '' for item in df_main[col.name]]
            elif col.columnType == 'INTEGER':
                df_main[col.name] = [int(item) if item==item else '' for item in df_main[col.name]]
            elif col.columnType == 'FILEHANDLEID':
                df_main[col.name] = [int(item) if (item!='' and item==item) else '' for item in df_main[col.name]]
            else:
                df_main[col.name] = [item if item==item else '' for item in df_main[col.name]]
        
        # Correcting the order of the columns while uploading
        df_main = df_main[[col.name for col in cols]]
        
        # Updaing schema and uploading
        schema = synapseclient.Schema(name=activity_, columns=cols, parent=uploadProjId)
        table = synapseclient.Table(schema, df_main)
        table = syn.store(table)
        table = syn.setProvenance(table.schema.id , activity = synapseclient.activity.Activity(used = all_tables[activity_]))
