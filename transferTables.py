import synapseclient
import numpy as np
import pandas as pd
from synapsebridgehelpers import tableWithFileIds
from groupTableActivity import groupTableActivity

def transferTables(syn,sourceProjId, uploadProjId, extId_Str = ''):
    
    # List of tables sorted by activity(as defined in groupTabActivity)
    tables_list = groupTableActivity(syn,sourceProjId,extId_Str)

    # Iterate over each specific activity - specAct in tables_list
    for activity_ in tables_list:
        print(activity_)
        
        # list of all table ids corresponding to activity 
        activityTableIds = tables_list[activity_]
        result = tableWithFileIds(syn,table_id = activityTableIds[0], extIdStr = extId_Str)
        df_main = result['df']
        cols = result['cols']
        
        # appending the rest of the sorted tables if they exist
        for table_index in range(1, len(activityTableIds)):
            result = tableWithFileIds(syn,table_id = activityTableIds[table_index], extIdStr = extId_Str)
            df = result['df']
            cols = result['cols']
            df_main = df_main.append(df)
        
        # Correcting the order of the columns while uploading
        temp_cols = df_main.columns.tolist()
        df_main = df_main[temp_cols]
        
        # Updaing schema and uploading
        schema = synapseclient.Schema(name=activity_ +' extIdStr_' + extId_Str, columns=cols, parent=uploadProjId)
        table = synapseclient.Table(schema, df_main)
        table = syn.store(table)
        table = syn.setProvenance(table.schema.id , activity = synapseclient.activity.Activity(used = tables_list[activity_]))