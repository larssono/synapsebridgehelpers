%matplotlib inline

import synapseclient
import pandas as pd
import numpy as np
from collections import Counter
import matplotlib.pylab as plt
from datetime import datetime

###########################################################################################

def healthCodeRecords(df, returnType = 'series'):
    """returns number of records per healthCode """
    try:
        sortedSeries = df['healthCode'].value_counts()        
        if returnType == 'series':
            return sortedSeries
        else:
            return dict(sortedSeries)
    except:
        print('The given dataframe does not have a column by that name')    
        
###########################################################################################

def recordsVsHealthCodes(df):
    """returns two lists 'nrecords' and 'nhealthcodes' which are number of records and
    the corresponding number of healthCodes with that many records"""
    try:
        healthCodes = (df['healthCode'].value_counts())
        countedHealthCodes = dict(Counter(healthCodes))
        x,y = zip(*sorted(countedHealthCodes.items()))
        return {'nrecords':x, 'nhealthcodes':y}
    except:
        print('The given dataframe does not have the column healthCodes')
        
###########################################################################################        
        
def plotRecordsVsHealthCodes(syn, table_id, nbins = 10, scale = 'linear'):
    """Plots the number of records vs the number of healthcodes that have that 
    many records"""
    try:
        results = syn.tableQuery('SELECT * FROM ' + table_id)
        df =  results.asDataFrame()
        plt.figure(figsize = (16,9))
        df.groupby('healthCode')['recordId'].count().hist(bins = nbins)
        plt.xlabel('#records', fontsize = 15)
        plt.ylabel('#healthcodes', fontsize = 15)
        plt.title('#Records vs #Healthcodes with that many records', fontsize = 18)
        if scale == 'log':
            plt.xscale('log')
            plt.yscale('log')

        plt.show()
    except:
        print('The given dataframe does not have the column healthcodes')
        
###########################################################################################
# Distribution of records over time of upload

def plotRecordDistribution(syn, table_id, timeline = 'month'):
    """plots records distribution according to timeline specified, options include
    'month', 'date', 'year', given the date is of the form yyyy-mm-dd"""
    
    # Fetch the table from synapse
    results = syn.tableQuery('SELECT * FROM ' + table_id)
    df =  results.asDataFrame()

    try:
        
        # Get the uploadDates
        uploadDate = df['uploadDate']
        uploadDate = list(uploadDate)
        uploadDate.sort()
        
        # Converting the uploadDate to the format of required frequency of sampling (month, day or year)
        if timeline == 'month':
            for index in range(0,len(uploadDate)):
                uploadDate[index] = uploadDate[index][0:-3]
        elif timeline == 'year':
            for index in range(0, len(uploadDate)):
                uploadDate[index] = uploadDate[index][0:4]
        elif timeline != 'date':
            print('unidentified timeline specified, reverting to default timline by month')
            timeline = 'month'
            for index in range(0,len(uploadDate)):
                uploadDate[index] = uploadDate[index][0:-3]
        
        
        ######################################################################################
        # I did not use pd.series.resample here as I was unable to implement it on this data #
        # even after trying to convert it into datetime format using pd.index.to_datetime    # 
        # if someone can solve this, it would be great !                                     #
        ######################################################################################
                
        # timeperiod vs number of occurances of that, using inbuilt Counter from collections module
        uploadDateCounts = Counter(uploadDate)
        
        # Convert into two separate lists for plotting purposes
        time_period, numberOfCounts = zip(*uploadDateCounts.items())
        
        # plotting stuff
        x_pos = np.arange(len(time_period))
        plt.figure(figsize=(16,9))
        plt.bar(x_pos,numberOfCounts)
        if timeline == 'date':
            plt.xticks(x_pos[0:len(x_pos):10],time_period[0:len(x_pos):10], rotation = 'vertical')
        else:
            plt.xticks(x_pos,time_period, rotation = 'vertical')        
        plt.xlabel(timeline, fontsize = 15)
        plt.ylabel('Number of submissions', fontsize = 15)
        plt.title('Submissions per ' + timeline, fontsize = 18)
        plt.xticks(fontsize = 12)
        plt.yticks(fontsize = 12)
        plt.show()
    
    except:
        print('Given table does not have column uploadDate')
    
###########################################################################################

# Number of records vs Days since enrollment
def plotRecordsVsDaysSinceEnrollment(syn,table_id,stepsize = 10):
    
    """Plots the number of records vs days since enrollment, by normalizing the submissions per healthcode according
    to the date they joined the study"""

    # Number of days passed between two dates
    def daysPassed(beforeDate, afterDate):

        # Specify the date format of the data we have
        date_format = "%Y-%m-%d"

        # Using inbuilt datetime function from the module datetime
        a = datetime.strptime(beforeDate, date_format)
        b = datetime.strptime(afterDate, date_format)
        delta = b - a
        return delta.days 
    
    # Fetch the table from synapse
    results = syn.tableQuery('SELECT * FROM ' + table_id)
    df =  results.asDataFrame()

    try:
        # generating list of healthcodes vs upload dates associated with that healthcode
        groupedByHealthCode = df.groupby(by = 'healthCode')['uploadDate'].apply(list)
        daysNormalizedByStartDate = list(groupedByHealthCode)
        daysVsNumberOfRecords = {}

        maxNumberOfDays = 0 # To keep track of missing days

        # looping over each healthCode
        for index in range(0, len(daysNormalizedByStartDate)):
            tempDaysList = daysNormalizedByStartDate[index]
            tempDaysList.sort()

            # For each healhcode converting the dates to days normalized by the enrollmentdate
            tempDaysList = [daysPassed(tempDaysList[0], tempDaysList[i]) for i in range(0,len(tempDaysList))]

            # Counting the records per day (after being normalized by enrollmentdate)
            for numberOfDays in tempDaysList:
                if (numberOfDays) in daysVsNumberOfRecords:
                    daysVsNumberOfRecords[(numberOfDays)] = daysVsNumberOfRecords[(numberOfDays)]+1
        #            maxNumberOfDays = max(maxNumberOfDays, numberOfDays)
                else:
                    daysVsNumberOfRecords[(numberOfDays)] = 1
                    maxNumberOfDays = max(maxNumberOfDays, numberOfDays)

        #Fill in missing days for consistency in the plot
        if maxNumberOfDays != len(daysVsNumberOfRecords):
            for dayNumber in range(0, maxNumberOfDays):
                if (dayNumber not in daysVsNumberOfRecords):
                    daysVsNumberOfRecords[dayNumber] = 0

        # Sorting the dictionary according to days from enrollmentdate
        daysVsNumberOfRecords = sorted(daysVsNumberOfRecords.items())

        # unpacking to days since enrollment and number of records on that day for plotting purposes
        daysEnrollment, numberOfCounts = zip(*daysVsNumberOfRecords) 

        # plotting stuff
        x_pos = np.arange(len(daysEnrollment))
        plt.figure(figsize=(16,9))
        plt.bar(x_pos,numberOfCounts)
        plt.xticks(x_pos[0:len(x_pos):stepsize],daysEnrollment[0:len(x_pos):stepsize], rotation = 'vertical')
        plt.xlabel('Days Since Enrollment', fontsize = 15)
        plt.ylabel('Number of Submissions', fontsize = 15)
        plt.title('#Records vs Days since Enrollment', fontsize = 18)
        plt.show()

    except:
        print('The given table does not have the column(s) healthCode and/or uploadDate')
