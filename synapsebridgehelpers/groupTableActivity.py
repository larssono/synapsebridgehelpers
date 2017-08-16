import synapsebridgehelpers
       
    
def groupTableActivity(syn,sourceProjId):
    """ Groups all table versions according to corresponding activities """

    # this function checks if the string is named like this '********-v*', 
    # and returns the check(True if -v** is version) and the position of the '-v' in the string
    def checkIfVersionFile(string):
        
        index_1 = string.find('-v') 
        #Returns -1 if it does not find the string '-v'
        if index_1 <0: 
            return {'flag' : 0, 'index' : index_1}
        
        # To see if the next index after -v is an integer, to sort out names like -voice etc.,
        # Assuming version naming is like -v** where ** are integers
        elif string[index_1+2] in ['1','2','3','4','5','6','7','8','9','0']: 
            return {'flag' : 1, 'index' : index_1}
        
        # File with no version tag in the name
        else:
            return {'flag' : 0, 'index' : index_1}

    # Get all tables fromt the given sourceProjId 
    # NOTE: Tables named 'parkinson-appVersion' and 'parkinson-status' are excluded because of get_tables
    all_tables = synapsebridgehelpers.tableHelpers.get_tables(syn,sourceProjId)
    
    # Create lists of table names and table ids
    table_names = all_tables['table.name']
    table_ids = all_tables['table.id']
    
    # names mapped to Ids and stored as a dict
    nameIdMap = dict(zip(table_names, table_ids))
    
    # Generate a list with tables sorted by activity            
    table_activities = {}
    for element in table_names:
        flagIndexElement = checkIfVersionFile(element)
        if flagIndexElement['flag']:
            index_1 = flagIndexElement['index']
            if element[0:index_1] in table_activities:
                table_activities[element[0:index_1]].append(nameIdMap[element])
            else:
                table_activities[element[0:index_1]] = [nameIdMap[element]]
        else:
            table_activities[element] = [nameIdMap[element]]
    
    return table_activities
    
    

