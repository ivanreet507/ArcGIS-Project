#!/usr/bin/env python
# coding: utf-8

# ## Welcome to your notebook.
# 

# #### Gets all users in arcgis and adds their data to users.csv

# In[1]:


# Logger script that logs errors from ArcGIS Enterprise/Online - Migrating User Content and Groups.py script
from arcgis.gis import GIS
import logging
import pathlib
import os
import datetime


class Logger(object):
    def __init__(self):
        self.outputToFile = True

    def setLogging(self, logFileName):

        # Change the working directory to our script's location
        currentScriptDirectory = os.path.dirname(os.path.realpath(__file__))
        os.chdir(currentScriptDirectory)

        # Create the logs folder if it doesn't already exist
        logsDirectory = pathlib.Path.cwd()
        logsDirectory = logsDirectory / 'logs'
        logsDirectory.mkdir(parents=True, exist_ok=True)

        # Get the log file with the date appended to it and set up basic logging
        logFilePath = logsDirectory / f'{logFileName} Log File.txt'
        if os.path.exists(logFilePath):
           os.remove(logFilePath)
        logging.basicConfig(filename=str(logFilePath), level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger()

    def setLevel(self, level):
        if level.upper() == "DEBUG":
            self.logger.setLevel(logging.DEBUG)
        elif level.upper() == "INFO":
            self.logger.setLevel(logging.INFO)
        elif level.upper() == "WARN":
            self.logger.setLevel(logging.WARN)
        elif level.upper() == "ERROR":
            self.logger.setLevel(logging.ERROR)
        elif level.upper() == "CRITICAL":
            self.logger.setLevel(logging.CRITICAL)

    def debug(self, message, exception=None):
        output = self.formatMessage(message, exception)
        if logging.getLevelName("DEBUG") >= self.logger.level:
            arcpy.AddMessage(output)
            if self.outputToFile:
                self.logger.debug(output)

    def info(self, message, exception=None):
        output = self.formatMessage(message, exception)
        if logging.getLevelName("INFO") >= self.logger.level:
            arcpy.AddMessage(output)
            if self.outputToFile:
                self.logger.info(output)

    def warn(self, message, exception=None):
        output = self.formatMessage(message, exception)
        if logging.getLevelName("WARN") >= self.logger.level:
            arcpy.AddWarning(output)
            if self.outputToFile:
                self.logger.warn(output)

    def error(self, message, exception=None):
        output = self.formatMessage(message, exception)
        if logging.getLevelName("ERROR") >= self.logger.level:
            arcpy.AddWarning(output)
            if self.outputToFile:
                self.logger.error(output)

    def critical(self, message, exception=None):
        output = self.formatMessage(message, exception)
        if logging.getLevelName("CRITICAL") >= self.logger.level:
            arcpy.AddError(output)
            if self.outputToFile:
                self.logger.critical(output)

    def formatMessage(self, message, exception):
        if exception:
            return f"{message}\n{exception}"

        return message
    
    
# This script writes all built-in user accounts and their associated groups, and add-on licenses to a CSV file
# The CSV will be used in the 'ArcGIS  Online - Migrating User Content and Groups.py

import csv, json, datetime
import requests 
from arcgis.gis import GIS

# Variables
portal = 'https://calpoly.maps.arcgis.com/'                                                             # AGOL URL
username = 'dle79_calpoly'                                                          # Admin Username
password = 'Food3663!'                                                                         # Admin Password
csvFile = r'/arcgis/home/users.csv'       # Output CSV File

# # Set Logging
# logger = Logger()
# logger.setLogging("Built-In Users")
# logger.setLevel('ERROR')

def convert_time(time_value):
    # remove sub-second values
    converted_time = int(time_value//1000)
    timestamp = datetime.datetime.fromtimestamp(converted_time)
    converted_time = timestamp.strftime('%Y-%m-%d %H:%M:%S')
    return converted_time


if __name__ == '__main__':
    gis = GIS(portal, username, password)
    print(f'Logged in as: {gis.properties.user.username}')

    # Get Token
    token = gis._con.token

    users = gis.users.search(max_users=6500, exclude_system=True)
    user_list = []
    for user in users:
        if user.provider != 'enterprise':
            print(f'Gathering info for user: {user.username}')
            if user.created > 0:
                created = convert_time(user.created)
            else:
                created = ''
            if user.lastLogin > 0:
                last_login = convert_time(user.lastLogin)
            else:
                last_login = 'Never'
            try:
                applist = []
                url = f'https://www.arcgis.com/sharing/rest/community/users/{user.username}/provisionedListings'
                params = {'f': 'pjson', 'token': token}
                r = requests.post(url, data=params)
                response = json.loads(r.content)
                for item in response['provisionedListings']:
                    applist.append(item['title'])
                applist = "; ".join(applist)
            except Exception as e:
                logger.error(f"\tError occurred getting provisions for {user.username}. {e}")
                applist = ''

            grouplist = []
            for g in user.groups:
                try:
                    grouplist.append(g.title)
                except:
                    pass
            grouplist = "; ".join(grouplist)


            # Configure first and last name
            try:
                firstName = user.firstName
            except:
                firstName = user.fullName.split(" ")[0]
            try:
                lastName = user.lastName
            except:
                lastName = user.fullName.split(" ")[-1]

            # Configure IDP Username
            idpUsername = user.email

            if user.level == '1':
                user.role = 'viewer'
                user_list.append(
                    [user.username, idpUsername, firstName, lastName, user.email, created, last_login,
                     user.level, user.userLicenseTypeId, user.role, user.storageUsage, applist, grouplist])
            else:
                user_list.append([user.username, idpUsername, firstName, lastName, user.email, created, last_login,
                                  user.level, user.userLicenseTypeId, user.role,  user.storageUsage, applist, grouplist])

            with open(csvFile, 'w', newline='') as myFile:
                writer = csv.writer(myFile)
                writer.writerow(['Username', 'IDP Username', 'First Name', 'Last Name', 'Email', 'Created',
                                 'Last Login', 'Level', 'License Type', 'Role', 'Storage Usage','Apps', 'Groups'])
                for user in user_list:
                    writer.writerow(user)


# #### Cleaning Data

# In[2]:


pip install pandasql


# In[8]:


import pandas as pd
import pandasql as ps

#Import users spreadsheet as a data table in python
df_users = pd.read_csv("/arcgis/home/users.csv")

#Rename columns to be only one word
df_usersclean = df_users.rename(columns = {"Last Login": "LastLogin", 
                                               "First Name": "FirstName",
                                               "Last Name": "LastName",
                                               "Storage Usage": "StorageUsage"
                                               })


# #### Seperate No Login Users

# In[10]:


#gets all the info of those who haven't logged in ever and their account was created before 7/1/2023
df_noLogin = ps.sqldf("SELECT * FROM df_usersclean WHERE Created < '2023-07-01' AND LastLogin = 'Never'", globals())



# #### Users with Multiple accounts

# In[11]:


#creates list of emails associated multiple accounts
df_multAccount = ps.sqldf("""SELECT Email, COUNT(*) AS NumAccounts
FROM df_usersclean
GROUP BY Email
HAVING COUNT(*) > 1
ORDER BY NumAccounts""", globals())


# In[12]:


df_multAccountCount = ps.sqldf("""SELECT SUM(NumAccounts)
FROM df_multAccount""", globals())


# In[13]:


#gets info of all accounts with multiple users
df_multAccountInfo = ps.sqldf("""SELECT * , df_usersclean.Email AS realEmail
FROM df_usersclean 
LEFT JOIN df_multAccount 
ON df_usersclean.Email = df_multAccount.Email""")


# In[14]:


print(df_multAccountCount)


# In[15]:


df_usersMult = ps.sqldf("""SELECT *
FROM df_multAccountInfo
WHERE Username LIKE '%_calpoly'""", globals())
print(df_usersMult)


# #### Not @calpoly.edu

# In[16]:


df_notCalPolyEDU = ps.sqldf("""SELECT *
FROM df_usersclean
WHERE Email NOT LIKE "%@calpoly.edu"
ORDER BY Created
""", globals())

print(df_notCalPolyEDU)


# #### Data Summed Up

# In[17]:


df_marked = ps.sqldf("""SELECT *,

COALESCE(NumAccounts, 1) AS numberAccounts,

CASE 
WHEN Created < '2023-07-01' AND LastLogin = 'Never' THEN 'OldNoLoginUser'
ELSE 'LoggedInUser'
END AS UserUse,

CASE
WHEN numAccounts > 1 AND Username LIKE '%_calpoly' THEN 'MultAccount_CalPoly'
WHEN numAccounts > 1  THEN 'MultAccountNOT_CalPoly'
ELSE 'SingleAccount'
END AS multAccount,

CASE
WHEN realEmail NOT LIKE "%@calpoly.edu" THEN 'not@CalPoly.edu'
ELSE "@calpoly.edu"
END AS EmailType

FROM df_multAccountInfo""", globals())

df_marked = df_marked.drop(columns = {"NumAccounts", "Email"})

print(df_marked)


# In[18]:


print(ps.sqldf("""SELECT *
FROM df_marked
WHERE multAccount = 'MultAccountNOT_CalPoly'
GROUP BY realEmail""", globals()))


# In[19]:


df_marked.to_csv('usersUpdated.csv')


# #### OrgMember xlsx Data

# In[20]:


df_orgMember = pd.read_excel('/arcgis/home/OrganizationMembers_2023-10-29.xlsx')
print(df_orgMember)


# In[21]:


print(ps.sqldf("""SELECT *
FROM df_orgMember
WHERE itemsOwned + GroupsOwned = 0""", globals()))


# In[ ]:




