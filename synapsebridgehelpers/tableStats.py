%matplotlib inline

import synapseclient
import pandas as pd
import numpy as np
from collections import Counter
import matplotlib.pylab as plt
from datetime import datetime

def healthCodeRecords(table_id, returnType = 'series'):
    """returns number of records per healthCode """
    try:
        sortedSeries = df['healthCode'].value_counts()        
        if returnType == 'series':
            return sortedSeries
        else:
            return dict(sortedSeries)
    except:
        print('The given dataframe does not have a column by that name')    

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
        
        
def recordDistribution(df, timeline = 'month'):
    """plots records distribution according to timeline specified, options include
    'month', 'date', 'year', given the date is of the form yyyy-mm-dd"""
    
    uploadDate = df['uploadDate']
    uploadDate = list(uploadDate)
    uploadDate.sort()

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
        
    uploadDateCounts = Counter(uploadDate)
    time_period, numberOfCounts = zip(*uploadDateCounts.items()) 
    x_pos = np.arange(len(time_period))
    plt.figure(figsize=(16,9))
    plt.bar(x_pos,numberOfCounts)
    if timeline == 'date':
        plt.xticks(x_pos[0:len(x_pos):10],time_period[0:len(x_pos):10], rotation = 'vertical')
    else:
        plt.xticks(x_pos,time_period, rotation = 'vertical')        
    plt.xlabel(timeline)
    plt.ylabel('Number of submissions')
    plt.show()
    
    


# Number of records vs Days since enrollment
def recordsVsDaysSinceEnrollment(df):
    
    # Returns the number of days between twodays
    def daysPassed(beforeDate, afterDate):
        date_format = "%Y-%m-%d"
        a = datetime.strptime(beforeDate, date_format)
        b = datetime.strptime(afterDate, date_format)
        delta = b - a
        return delta.days 
    
    groupedByHealthCode = df.groupby(by = 'healthCode')['uploadDate'].apply(list)
    daysNormalizedByStartDate = list(groupedByHealthCode)
    daysVsNumberOfRecords = {}

    for index in range(0, len(daysNormalizedByStartDate)):
        tempDaysList = daysNormalizedByStartDate[index]
        tempDaysList.sort()
        tempDaysList = [daysPassed(tempDaysList[0], tempDaysList[i]) for i in range(0,len(tempDaysList))]
        for numberOfDays in tempDaysList:
            if (numberOfDays) in daysVsNumberOfRecords:
                daysVsNumberOfRecords[(numberOfDays)] = daysVsNumberOfRecords[(numberOfDays)]+1
            else:
                daysVsNumberOfRecords[(numberOfDays)] = 1
    daysVsNumberOfRecords = sorted(daysVsNumberOfRecords.items())
    daysEnrollment, numberOfCounts = zip(*daysVsNumberOfRecords) 
    x_pos = np.arange(len(daysEnrollment))
    plt.figure(figsize=(16,9))
    plt.bar(x_pos,numberOfCounts)
    plt.xticks(x_pos[0:len(x_pos):10],daysEnrollment[0:len(x_pos):10], rotation = 'vertical')
    plt.xlabel('Days Since Enrollment')
    plt.ylabel('Number of Submissions')
    plt.show()



