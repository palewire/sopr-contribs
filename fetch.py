#!/usr/bin/env python
"""
A script that fetches, parses and archives the XML data dumps of lobbyist's
political contributions published by The Senate Office of Public Records.

Zips files containing the XML are:
1. Downloaded and unzipped.
2. Parsed out into flat text files and stored in a timestamped folder structure.
3. Imported to a SQLite database.

The ultimate goal is for a series of SQL statements to scrub and cut the data
to account for flaws in the reporting system first uncovered by Bill Allison
and Anupama Narayanswamy of The Sunlight Foundation.

Sunlight study:
http://realtime.sunlightprojects.org/2008/08/14/mark-warner-biggest-recipient-of-lobbyist-dough-new-disclosures-show-so-far/

Simple analysis tasks could then be scripted to output as schedule XLS dumps,
email alerts or maybe even Django-ifed HTML. 

Source URL:
http://www.senate.gov/legislative/Public_Disclosure/contributions_download.htm

Dependencies: BeautifulSoup, Pysqlite2

The MIT License
 
Copyright (c) 2008 Ben Welsh
 
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
  
"""
__author__ = "Ben Welsh <ben.welsh@gmail.com>"
__date__ = "$Date: 2008/08/17 $"
__version__ = "$Revision: 0.1 $"

import datetime
import os
import urllib
import re
import codecs
import zipfile
import string
try:
    from BeautifulSoup import BeautifulStoneSoup, BeautifulSoup
except ImportError:
    print """
IMPORT ERROR: Required module not found: Beautiful Soup.
Installation instructions:
If you have easy_install, enter
"sudo easy_install BeautifulSoup"
via your shell.
Otherwise, the source can be downloaded from
http://www.crummy.com/software/BeautifulSoup/
"""
    raise SystemExit
try:
    from pysqlite2 import dbapi2 as sqlite
except ImportError:
    print """
IMPORT ERROR: Required module not found: Pysqlite.
Visit http://pysqlite.org and download the latest module.
"""
    raise SystemExit
    
#"""Create an archive folder structure using the current datetime. Returns path."""
###Setting timestamps
now = datetime.datetime.now()
datestamp = "%s-%s-%s" % (now.year, now.month, now.day)
timestamp = "%sh%sm%ss" % (now.hour, now.minute, now.second)
sqlitestamp = "%s-%s-%s %s:%s:%s" % (now.year, now.month, now.day,
                                     now.hour, now.minute, now.second)

###Setting directory variables, creating archive folder structure
working_directory = "."
data_directory = os.path.join(working_directory, 'data')

if os.path.isdir(data_directory): 
    print "Data directory already exists at %s" % data_directory
else: 
    os.mkdir(data_directory)
    print "Creating data directory at %s" % data_directory

todays_data_subdirectory = os.path.join(data_directory, datestamp)

if os.path.isdir(todays_data_subdirectory): 
    print "Today's data subdirectory already exists at %s" % todays_data_subdirectory
else: 
    os.mkdir(todays_data_subdirectory)
    print "Creating today's data subdirectory at %s" % todays_data_subdirectory

this_scripts_data_subdirectory = os.path.join(todays_data_subdirectory, timestamp)

if os.path.isdir(this_scripts_data_subdirectory): 
    print "This script's data subdirectory already exists at %s" % this_scripts_data_subdirectory
else: 
    os.mkdir(this_scripts_data_subdirectory)
    print "Creating this script's data subdirectory at %s" % this_scripts_data_subdirectory

##Open files for writing out.
filings_path = os.path.join(this_scripts_data_subdirectory, 'filings.txt')
lobbyists_path = os.path.join(this_scripts_data_subdirectory, 'lobbyists.txt')
contribs_path = os.path.join(this_scripts_data_subdirectory, 'contribs.txt')

filings_file = codecs.open(filings_path, "w", "utf-8")
lobbyists_file = codecs.open(lobbyists_path, "w", "utf-8")
contribs_file = codecs.open(contribs_path, "w", "utf-8")

##Visiting SOPR to grab the zip downloads
url = 'http://www.senate.gov/legislative/Public_Disclosure/contributions_download.htm'
http = urllib.urlopen(url)
soup = BeautifulSoup(http)
anchor_tags = soup.findAll('a')
zip_links = []
for a in anchor_tags:
    href = a['href']
    if re.search('(.*).zip', href):
        zip_links.append(href)

for zip_link in zip_links:
    zip_name = zip_link.split('/')[-1]
    zip_path = os.path.join(this_scripts_data_subdirectory, zip_name)
    urllib.urlretrieve(zip_link, zip_path)
    print "Downloaded %s " % zip_name

    ##Unzip file
    try:
        zip = zipfile.ZipFile(zip_path)
        for file in zip.namelist():
            print "Unzipping %s" % file
            f = open(os.path.join(this_scripts_data_subdirectory, file), 'wb')
            f.write(zip.read(file))
            f.close()
    except:
        print "Failed to unzip %s" % zip_name

##Snatching XML files for parsing
this_scripts_downloads = os.listdir(this_scripts_data_subdirectory)
this_scripts_xml_files = []

for file in this_scripts_downloads:
    if re.search(".xml", file): 
        this_scripts_xml_files.append(file)

filing_id = 0

##Parsing XML files
for xml_file_name in this_scripts_xml_files:
    print "Processing %s" % xml_file_name

    xml_file = os.path.join(this_scripts_data_subdirectory, xml_file_name)
    xml = open(xml_file, "r")

    soup = BeautifulStoneSoup(xml, selfClosingTags=['lobbyist', 'contribution', 'registrant'])
    
    ##Parsing filing data
    for f in soup.publicfilings.findAll('filing'):
    
        filing_id = filing_id + 1
    
        filing = []
    
        filing.append("%s" % filing_id)
        filing.append(xml_file_name)
        
        try: filing.append(f['id'])
        except: filing.append('null')
    
        try: filing.append(f['year']) 
        except: filing.append('null')
    
        try: filing.append(f['received']) 
        except: filing.append('null')

        try: filing.append(f['type']) 
        except: filing.append('null')

        try: filing.append(f['period']) 
        except: filing.append('null')
    
        try: filing.append(f.registrant['registrantid']) 
        except: filing.append('null')
    
        try:
           raw_registrant = f.registrant['registrantname']
           split_registrant = raw_registrant.split('&#x0D;&#x0A;')
           clean_registrant = " ".join(split_registrant)
           filing.append(clean_registrant)
        except: filing.append('null') 
   
        try:
           raw_address = f.registrant['address']
           split_address = raw_address.split('&#x0D;&#x0A;')
           clean_address = " ".join(split_address)
           filing.append(clean_address)
        except: filing.append('null')   

        try:
           raw_country = f.registrant['registrantcountry']
           split_country = raw_country.split('&#x0D;&#x0A;')
           clean_country = " ".join(split_country)
           filing.append(clean_country)
        except: filing.append('null') 
            
        print >> filings_file, '|'.join(filing)

        try:
            ##Parsing lobbyist names
            for l in f.findAll('lobbyist'):
                lobbyist = []
                lobbyist.append("%s" % filing_id)
                lobbyist.append(xml_file_name)
                lobbyist.append(f['id'])
                try:
                    raw_name = l['lobbyistname']
                    split_name = raw_name.split('&#x0D;&#x0A;')
                    clean_name = " ".join(split_name)
                    lobbyist.append(clean_name) 
                except: lobbyist.append('null')
                
                print >> lobbyists_file, '|'.join(lobbyist)
        except:
            print "Failed parsing lobbyist record for filing %s" % f['id']
    
        try:
            ##Parsing contributions data
            for c in f.contributions:
                             
                contrib = []
                contrib.append("%s" % filing_id)
                contrib.append(xml_file_name)
                contrib.append(f['id'])     

                try: contrib.append(c['contributor']) 
                except: contrib.append('null')  

                try: contrib.append(c['contributiontype']) 
                except: contrib.append('null')    

                try:
                   raw_payee = c['payee']
                   split_payee = raw_payee.split('&#x0D;&#x0A;')
                   clean_payee = " ".join(split_payee)
                   contrib.append(clean_payee)
                except: contrib.append('null')   

                try:
                   raw_honoree = c['honoree']
                   split_honoree = raw_honoree.split('&#x0D;&#x0A;')
                   clean_honoree = " ".join(split_honoree)
                   contrib.append(clean_honoree)
                except: contrib.append('null')   

                try: contrib.append(c['amount']) 
                except: contrib.append('null')   

                try: contrib.append(c['contributiondate']) 
                except: contrib.append('null')

                print >> contribs_file, '|'.join(contrib)

        except:
            pass
        
##Closing out files
filings_file.close()
lobbyists_file.close()
contribs_file.close()

con = sqlite.connect(os.path.join(this_scripts_data_subdirectory, "contribs"))
cur = con.cursor()

## Creating Sqlite tables
## Will ultimately need to convert date fields from varchar to datetime.
create_tables = """
    create table if not exists
    filing(
        artificial_filing_id    integer,
        xml_file_name           varchar(100),
        sopr_filing_id          varchar(100),
        year                    integer,
        received                varchar(100),
        type                    varchar(100),
        period                  varchar(100),
        registrant_id           integer,
        registrant_name         varchar(100),
        registrant_address      varchar(500),
        registrant_country      varchar(100),
        insert_datetime         datetime
    );

    create table if not exists
    contrib(
        artificial_filing_id    integer,
        xml_file_name           varchar(100),
        sopr_filing_id          varchar(100),
        contributor             varchar(100),
        contribution_type       varchar(100),
        payee                   varchar(200),
        honoree                 varchar(200),
        amount                  integer,
        contribution_date       varchar(100), 
        insert_datetime         datetime
    );

    create table if not exists
    lobbyist(
        artificial_filing_id    integer,
        xml_file_name           varchar(100),
        sopr_filing_id          varchar(100),
        lobbyist_name           varchar(200),
        insert_datetime         datetime
    );"""

cur.executescript(create_tables)

##Reopening flat files for reading so they can be inserted into the db
filings_file = codecs.open(filings_path, "r", "utf-8")
lobbyists_file = codecs.open(lobbyists_path, "r", "utf-8")
contribs_file = codecs.open(contribs_path, "r", "utf-8")

print "Inserting filings"
for line in filings_file:
    record = line.split('|')
    record.append(sqlitestamp)
    
    insert_record = """
    insert into filing(
        artificial_filing_id,
        xml_file_name,
        sopr_filing_id,
        year,
        received,
        type,
        period,
        registrant_id,
        registrant_name,
        registrant_address,
        registrant_country,
        insert_datetime
        )
    values (
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?
    );"""

    cur.execute(insert_record, record)

con.commit()

print "Inserting contribs"
for line in contribs_file:
    record = line.split('|')
    record.append(sqlitestamp)
    
    insert_record = """
    insert into contrib(
        artificial_filing_id,
        xml_file_name,
        sopr_filing_id,
        contributor,
        contribution_type,
        payee,
        honoree,
        amount,
        contribution_date,
        insert_datetime
        )
    values (
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?
    );"""

    cur.execute(insert_record, record)

con.commit()

print "Inserting lobbyists"
for line in lobbyists_file:
    record = line.split('|')
    record.append(sqlitestamp)
    
    insert_record = """
    insert into lobbyist(
        artificial_filing_id,
        xml_file_name,
        sopr_filing_id,
        lobbyist_name,
        insert_datetime
        )
    values (
        ?,
        ?,
        ?,
        ?,
        ?
    );"""

    cur.execute(insert_record, record)

con.commit()

con.close()

##Closing out files
filings_file.close()
lobbyists_file.close()
contribs_file.close()

