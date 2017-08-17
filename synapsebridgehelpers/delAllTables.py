import pandas as pd

def delAllTables(syn,projectId):
    """ Deletes all tables in a given project """
    all_tables = syn.getChildren(projectId,includeTypes=[u'table'],sortBy=u'NAME', sortDirection=u'ASC')
    df_table = pd.DataFrame(all_tables)
    
    if df_table.shape[0]*df_table.shape[1] == 0:
        print('No tables in the given project : ' + str(projectId))
    else:
    
        # find all unique table names in the list of tables, they will be named like parkinson-*****-v*
        #df_un = pd.unique(df_table['name'])
        df_un_id = pd.unique(df_table['id'])

        for i in df_un_id:
            schema = syn.get(str(i))
            syn.delete(schema)
        print('Done deleting all tables in the given project : ' + str(projectId))

