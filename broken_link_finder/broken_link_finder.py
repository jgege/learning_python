from BeautifulSoup import BeautifulSoup as BS
import urllib2
import re
import sqlite3 as lite
import random
import sys
import time
from time import strftime, gmtime

def concatTime(str):
    return "[%s] %s" % (strftime("%Y-%m-%d %H:%M:%S", gmtime()), str) 

# create valid db name from url
def convertUrlToDbName(url):
    deletable_strings = {
        'http://' : '', 
        'https://' : '',
    }
    
    for replace in deletable_strings:
        url = url.replace(replace, deletable_strings[replace])

    url = re.sub('[^0-9a-zA-Z]+', '_', url)
    url = re.sub('\_$', '', url)
    
    return url

# creates basic tables
def createTables(cur):
    cur.execute("CREATE TABLE IF NOT EXISTS \
        link (\
            id INTEGER PRIMARY KEY, \
            url TEXT, \
            http_status INTEGER, \
            crawl_status INTEGER, \
            follow INTEGER \
        )")

    cur.execute("CREATE TABLE IF NOT EXISTS \
        link_relation (\
            parent_id INTEGER, \
            child_id INTEGER \
        )")

    cur.execute("CREATE TABLE IF NOT EXISTS \
        link_skipped (\
            id INTEGER, \
            parent_id INTEGER, \
            href TEXT \
        )")

    con.commit()

def getAllLinksFromHtml(html, base_url):
    self_links = []
    for link in html.findAll('a'):
        if link.has_key('href'):
            href = link['href']
            if href.startswith('/') or href.startswith('./') or href.startswith(base_url):
                #print "Found internal link: " + link['href']
                self_links.append(\
                    {'link' : href, 'follow': 1, 'skip': False}\
                )
            elif href.startswith('http') :
                #print "Found external link: " + link['href']
                self_links.append(\
                    {'link' : href, 'follow': 0, 'skip': False}\
                )
            else:
                self_links.append(\
                    {'link' : href, 'follow': 0, 'skip': True}\
                )
                print "Skipping %s" % href.encode('utf-8')
    return self_links

def getContentFromUrl(url):
    url = url.encode('utf-8')
    try:
        response = urllib2.urlopen(url)
    except urllib2.HTTPError as e:
        return {'success' : False, 'code': e.code}
    except urllib2.URLError as e:
        print 'We failed to reach a server.'
        print 'Reason: ', e.reason
        
    content_type = response.info().getheader('Content-Type')
    if content_type.startswith('text/html') == False:
        return {'success': False, 'html' : None, 'code': response.getcode()}
    
    return {'success': True, 'html' : BS(response), 'code': response.getcode()}
    
def getUrlToCrawl(cur):
    cur.execute("SELECT url, follow FROM link WHERE crawl_status = 0 LIMIT 1")
    return cur.fetchone()
    
def isFirstRun(cur):
    cur.execute("SELECT COUNT(*) FROM link")
    count = cur.fetchone()

    if(count == None or count[0] < 1):
        return True
    return False

def normalizeUrl(url):
    if url.startswith('/'):
        return re.sub('\/$', '', site_base_url) + url
    return url

def saveLink(cur, url, http_status, crawl_status, follow):
    #print "before normalize: %s" % url
    url = normalizeUrl(url)
    #print "after normalize: %s" % url

    cur.execute("SELECT id FROM link WHERE url = ?", (url,))
    link_id = cur.fetchone()
    
    if link_id == None:
        cur.execute("\
            INSERT INTO link(url, http_status, crawl_status, follow) \
            VALUES(?, ?, ?, ?)", (url, http_status, crawl_status, follow)
        )
    else:
        id = link_id[0]
        cur.execute("\
            UPDATE link \
            SET http_status = ?, \
            crawl_status = ? \
            WHERE id = ?", (http_status, crawl_status, id)
        )
        
    return cur.lastrowid

def saveAllLinks(cur, links, parent_url):
    cur.execute("SELECT id FROM link WHERE url = ?", (parent_url ,))
    parent = cur.fetchone()
    
    if parent == None:
        print "Can't save relation because url not found: %" % parent_url 
        exit(1)
        
    parent_id = parent[0]
    
    print "Saving links:"
    for link in links:
        
        if link['skip'] == 1:
            saveSkippedHref(cur, parent_id, link['link'])
            continue

        url = normalizeUrl(link['link'])
        follow = link['follow']

        cur.execute("SELECT id FROM link WHERE url = ?", (url, ))
        exists = cur.fetchone()

        if exists == None:
            print "n ",
            link_id = saveLink(cur, url, None, 0, follow)
            saveLinkRelation(cur, parent_id, link_id)
        else:
            print "e ",
            saveLinkRelation(cur, parent_id, exists[0])
            continue

def saveLinkRelation(cur, parent_id, child_id):
    cur.execute("INSERT INTO link_relation VALUES(?, ?)", (parent_id, child_id))
   
def saveSkippedHref(cur, parent_id, href):
    cur.execute("INSERT INTO link_skipped(parent_id, href) VALUES(?, ?)", (parent_id, href))
    
# site to crawl
site_base_url = "http://example.com/"

# set db name and path
db_name = "blf_" + convertUrlToDbName(site_base_url) + ".db"
db_path = "db/"

# connect to db
con = lite.connect(db_path + db_name)

# main things
with con:
    # lets create a cursor and the base tables 
    cur = con.cursor()
    createTables(cur)
    
    # check to see if we need to crawl the base url
    first_run = isFirstRun(cur)
    
    run_count = 0
    
    while True:
        # try to get an url    
        url_to_crawl = site_base_url
        url_follow = 1
        if first_run == False:
            link_to_crawl = getUrlToCrawl(cur)
            if link_to_crawl == None:
                url_to_crawl = None
                url_follow = 0
            else:
                url_to_crawl = normalizeUrl(link_to_crawl[0])
                url_follow = link_to_crawl[1]
        else:
            # we save the base url to db
            saveLink(cur, site_base_url, None, 1, 1)
            con.commit()

        first_run = False
        # no more url, we've done everything
        if url_to_crawl == None:
            print "No more url to crawl"
            exit(0)

        print concatTime("Sending request to %s" % (url_to_crawl.encode('utf-8')))   
        response = getContentFromUrl(url_to_crawl)
        print concatTime("Response arrived!")
        
        if response['success'] == True and response['code'] == 200:
            # we get all links
            print concatTime('Successful request')
            saveLink(cur, url_to_crawl, response['code'], 3, None)
            con.commit()

            # only do this if follow is set to 1
            if url_follow == 1:
                links = getAllLinksFromHtml(response['html'], site_base_url)
                saveAllLinks(cur, links, url_to_crawl)
                con.commit()
        else:
            # something went wrong
            print concatTime("Something went wrong here: %s" %url_to_crawl.encode('utf-8') )
            saveLink(cur, url_to_crawl, response['code'], 2, None)
            con.commit()

        run_count += 1
        print ""
        print concatTime("%d. run completed" % run_count)
        wait_for = random.randint(1, 5)
        print concatTime("Waiting for %d sec" % wait_for)
        
        time.sleep(wait_for)
