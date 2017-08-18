import pandas as pd

def delAllTables(syn,projectId):
    """ Deletes all tables in a given project """
    all_tables = syn.getChildren(projectId,includeTypes=[u'table'],sortBy=u'NAME', sortDirection=u'ASC')
    df_table = pd.DataFrame(all_tables)
    
    # Checking to see if given project folder is empty 
    if df_table.shape[0]*df_table.shape[1] == 0:
        print('No tables in the given project : ' + str(projectId))
    else:
        # find all unique table ids in the project folder
        df_un_id = pd.unique(df_table['id'])
        
        # iterating to delete each table id
        for table_id in df_un_id:
            schema = syn.get(str(table_id))
            syn.delete(schema)
        print('Done deleting all tables in the given project : ' + str(projectId))

