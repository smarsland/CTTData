# These functions download data from the CTT website
# Written partly because their R code has issues

import requests, csv, gzip, os, glob, datetime

metadata={
    'host' : 'https://api.internetofwildlife.com' ,
    'projectdir' : '/station/api/projects',
    'stationdir' : '/station/api/stations',
    'filedir' : '/station/api/file-list',
    'getfiledir' : '/station/api/download-file',
    'token' : 'ec28800e77b3b2ce0993c51c2bf1a62b62e8ef1f369696a0a27ea98847f8b3c1',
    'projectid' : 200,
    'projectname' : 'Rat responses to landscape barriers and corridors',
    'stationid' : '6B9A01B19306',
    #'file_types' : ["data", "node-data", "gps", "log", "telemetry", "sensorgnome"],
    'file_types' : ["data", "node-data", "gps", "log"],
    'savepath' : '/home/marslast/Downloads/CTT/',
}

# Remember that response 200 is success. Then you have to get the result using .json()

# To get project id
def getProjectId(metadata):
    projects = requests.post(metadata['host']+metadata['projectdir'],json={'token':metadata['token']})
    projectid = None
    if projects.json()['projects'][0]['name'] == metadata['projectname']:
        projectid = projects.json()['projects'][0]['id']
    return projectid

# To get the station ID and date deployed (of the first)
def getStationId(metadata):
    stations = requests.post(metadata['host']+metadata['stationdir'],json={'token':metadata['token'], 'project-id':metadata['projectid']})
    stationid = stations.json()['stations'][0]['station']['id']
    date_deployed = stations.json()['stations'][0]['deploy-at']
    return stationid, date_deployed

# Specify a date
def getFiles(metadata,date):

    if date is None:
        stations = requests.post(metadata['host']+metadata['stationdir'],json={'token':metadata['token'], 'project-id':metadata['projectid']})
        date = stations.json()['stations'][0]['deploy-at']

    # Check if the folders exist, and otherwise make them
    if not os.path.exists(metadata['savepath']):
            os.makedirs(metadata['savepath'])
    for f in metadata['file_types']:
        if not os.path.exists(os.path.join(metadata['savepath'],f)):
            os.makedirs(os.path.join(metadata['savepath'],f))

    files = requests.post(metadata['host']+metadata['filedir'],json={'token':metadata['token'], 'project-id':metadata['projectid'], 'station-id':metadata['stationid'],'file-types':metadata['file_types'],'begin':date})
    #files = requests.post(metadata['host']+metadata['filedir'],json={'token':metadata['token'], 'project-id':metadata['projectid'], 'station-id':stations.json()['stations'][0]['station']['id'],'file-types':metadata['file_types'],'begin':date})

    for j in metadata['file_types']:
        for i in range(len(files.json()['files'][j])):
            file_id = files.json()['files'][j][i]['id']
            download = requests.post(metadata['host']+metadata['getfiledir'],json={'token':metadata['token'], 'project-id':metadata['projectid'], 'station-id':metadata['stationid'],'file-id':file_id})
            with open(os.path.join(metadata['savepath'],j,files.json()['files'][j][i]['name']),mode='wb') as f:
                f.write(gzip.compress(download.text.encode()))
            f.close()

def getLastDate(metadata):
    files = glob.glob(os.path.join(metadata['savepath'],'data','*.csv.gz'))
    dates = []
    for f in files:
        if datetime.datetime.strptime(f[-24:-14], "%Y-%m-%d").date() not in dates:
            dates.append(datetime.datetime.strptime(f[-24:-14], "%Y-%m-%d").date())

    return max(dates)

def getLastFile(metadata):
    files = glob.glob(os.path.join(metadata['savepath'],'data','*.csv.gz'))
    dates = []
    for f in files:
        dates.append(datetime.datetime.strptime(f[-24:-7],'%Y-%m-%d_%H%M%S'))
    lastFile = max(dates)
    return lastFile

def updateFiles(metadata):
    # Get the new files from the server

    lastFile = getLastFile(metadata)
    lastFile += datetime.timedelta(seconds=1)
    lastFile = lastFile.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    getFiles(metadata,lastFile)
    return lastFile

#startdate = '2023-05-01'

#files = requests.post(host+filedir,json={'token':token, 'project-id':projectid, 'station-id':stationid,'file-types':file_types,'begin':startdate})

# Assemble the raw data into a pandas table
# Also get the GPS coords, node data

