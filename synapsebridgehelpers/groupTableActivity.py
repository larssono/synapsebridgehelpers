import synapsebridgehelpers
       
    
def groupTableActivity(syn,sourceProjId, extIdStr = '', activityNameFilters=[]):
    """ Groups all table versions according to corresponding activities, 
    has option to find tables with certain externalIds. activityNameFilters is used 
    to remove the unneccesary strings in table names before grouping them together """
    
    all_tables = synapsebridgehelpers.get_tables(syn,sourceProjId,activityNameFilters)
    table_activities = dict(all_tables.groupby(by='simpleName')['table.id'].apply(list))
    
    if extIdStr == '':
        return table_activities
    else:
        temp_table_activities = {}           
        for element in table_activities:
            table_ids_activity = table_activities[element]
            for table_id in table_ids_activity:
                results = syn.tableQuery('select * from '+ table_id +' where externalId like "' + extIdStr + '%"')
                df = results.asDataFrame()
                if df.shape[0]*df.shape[1] > 0:
                    if element in temp_table_activities:
                        temp_table_activities[element].append(table_id)
                    else:
                        temp_table_activities[element] = [table_id]
                    
        table_activities = temp_table_activities
        
    return table_activities
