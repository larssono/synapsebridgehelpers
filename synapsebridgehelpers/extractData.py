import synapseclient
import synapseutils
from synapseclient import Schema, Column, Table, Row, RowSet, as_table_columns
import synapsebridgehelpers
syn = synapseclient.login('BridgeExporter', 'NFfexdck')

#Copy Tremor Data
#r = synapseutils.copy(syn, 'syn5734657', 'syn8443913')

#Get all tables in the Bridge mPower project
all_tables = get_tables(syn, 'syn3270406')

#Find the healthCodes of interest
healthCodes = syn.tableQuery('SELECT distinct healthCode FROM syn8444013')
healthCodes = list(healthCodes.asDataFrame()['healthCode'])

#Determine which tables have data for these healthCodes
all_tables = find_tables_with_data(syn, all_tables, healthCodes)
all_tables.sort_values('healthCodeCounts').tail(10)

## Copy enrollment Survey for specified healthCodes
data = syn.tableQuery("select * from syn5752774 where healthCode in ('"+
                      "','".join(healthCodes)+"')")
df = data.asDataFrame()
columns=as_table_columns(df)
columns[3]['columnType']='DATE'
columns[16]['columnType']='BOOLEAN'
columns[-1]['columnType']='FILEHANDLEID'
schema = Schema(name='EnrollmentSurvey', columns=columns, parent='syn8443913')
table = syn.store(Table(schema, df))
