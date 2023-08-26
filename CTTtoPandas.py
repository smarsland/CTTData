
# Read data from downloaded CTT files into Pandas dataframes
# Remove as many errors as possible

# The dataframes will be:
# gps, data, nodedata  -- ignore log

# nodedata holds the information about checkins from the nodes. It isn't immediately useful.
# gps holds the actual data from tags, and seems very badly named. It does have the GPS reads from the nodes as well. Weird.
# data seems to hold the data from the tags, but with less information. I don't understand the difference atm.


import glob, os, datetime
import pandas as pd

def getFileList(savedir,startdate,enddate=None):

    if enddate is None:
        enddate = datetime.date.today()

    gpsfiles = glob.glob(os.path.join(savedir,'gps','*.csv.gz'))
    ndfiles = glob.glob(os.path.join(savedir,'node-data','*.csv.gz'))
    dfiles = glob.glob(os.path.join(savedir,'data','*.csv.gz'))

    gpslist = []
    ndlist = []
    dlist = []

    for f in gpsfiles:
        if (datetime.datetime.strptime(f[-24:-14], "%Y-%m-%d").date() > startdate) & (datetime.datetime.strptime(f[-24:-14], "%Y-%m-%d").date() < enddate):
            gpslist.append(f)

    for f in ndfiles:
        if (datetime.datetime.strptime(f[-24:-14], "%Y-%m-%d").date() > startdate) & (datetime.datetime.strptime(f[-24:-14], "%Y-%m-%d").date() < enddate):
            ndlist.append(f)

    for f in dfiles:
        if (datetime.datetime.strptime(f[-24:-14], "%Y-%m-%d").date() > startdate) & (datetime.datetime.strptime(f[-24:-14], "%Y-%m-%d").date() < enddate):
            dlist.append(f)

    return gpslist, ndlist, dlist

def addData(gps,nodedata,data,gpslist,ndlist,dlist):

    if gps is None:
        gps = pd.read_csv(gpslist[0])
        gpslist.pop(0)

    if nodedata is None:
        gps = pd.read_csv(ndlist[0])
        ndlist.pop(0)

    if data is None:
        gps = pd.read_csv(dlist[0])
        dlist.pop(0)

    for f in gpslist:
        df = pd.read_csv(f)
        gps = pd.concat([gps,df],ignore_index=True)

    for f in ndlist:
        df = pd.read_csv(f)
        nodedata = pd.concat([nodedata,df],ignore_index=True)

    for f in dlist:
        df = pd.read_csv(f)
        data = pd.concat([data,df],ignore_index=True)

    return gps, nodedata, data
    
#savedir = '/home/marslast/Downloads/CTT/'
#startdate = "2023-06-17"
#enddate = "2023-06-19"

def saveDataFrames(savedir):
    gps.to_pickle(os.path.join(savedir,"GPS.pkl"))
    data.to_pickle(os.path.join(savedir,"data.pkl"))
    nodedata.to_pickle(os.path.join(savedir,"nodedata.pkl"))

def loadDataFrames(readdir):
    gps = pd.read_pickle(os.path.join(readdir,"GPS.pkl"))
    data = pd.read_pickle(os.path.join(readdir,"data.pkl"))
    nodedata = pd.read_pickle(os.path.join(readdir,"nodedata.pkl"))
