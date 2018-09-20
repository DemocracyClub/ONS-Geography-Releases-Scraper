import os
import requests
from polling_bot.brain import SlackClient, GitHubClient

# hack to override sqlite database filename
# see: https://help.morph.io/t/using-python-3-with-morph-scraperwiki-fork/148
os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'
import scraperwiki


SEND_NOTIFICATIONS = True

try:
    SLACK_WEBHOOK_URL = os.environ['MORPH_POLLING_BOT_SLACK_WEBHOOK_URL']
except KeyError:
    SLACK_WEBHOOK_URL = None

try:
    GITHUB_API_KEY = os.environ['MORPH_GITHUB_ISSUE_ONLY_API_KEY']
except KeyError:
    GITHUB_API_KEY = None


def post_slack_message(record):
    message = "New %s available at %s" % (record['title'], record['url'])
    slack = SlackClient(SLACK_WEBHOOK_URL)
    slack.post_message(message)


def raise_github_issue(repo, record):
    owner = 'DemocracyClub'
    title = "Import %s" % (record['title'])
    body = "@chris48s - New %s available at %s" % (record['title'], record['url'])
    github = GitHubClient(GITHUB_API_KEY)
    github.raise_issue(owner, repo, title, body)


def init():
    scraperwiki.sql.execute("""
        CREATE TABLE IF NOT EXISTS onspd (
            url TEXT,
            title TEXT,
            id TEXT);""")
    scraperwiki.sql.execute("CREATE UNIQUE INDEX IF NOT EXISTS onspd_id_unique ON onspd (id);")
    scraperwiki.sql.execute("""
        CREATE TABLE IF NOT EXISTS onsad (
            url TEXT,
            title TEXT,
            id TEXT);""")
    scraperwiki.sql.execute("CREATE UNIQUE INDEX IF NOT EXISTS onsad_id_unique ON onsad (id);")
    scraperwiki.sql.execute("""
        CREATE TABLE IF NOT EXISTS lgd (
            url TEXT,
            title TEXT,
            id TEXT);""")
    scraperwiki.sql.execute("CREATE UNIQUE INDEX IF NOT EXISTS lgd_id_unique ON lgd (id);")
    scraperwiki.sql.execute("""
        CREATE TABLE IF NOT EXISTS wards (
            url TEXT,
            title TEXT,
            id TEXT);""")
    scraperwiki.sql.execute("CREATE UNIQUE INDEX IF NOT EXISTS wards_id_unique ON wards (id);")


def scrape(url, table):
    res = requests.get(url)
    if res.status_code != 200:
        res.raise_for_status()
    data = res.json()

    try:
        results = data['results']
    except KeyError:
        results = data['data']

    for result in results:
        record = {
            'id': result['id'],
            'url': "http://geoportal.statistics.gov.uk/datasets/" + result['id'],
        }
        try:
            record['title'] = result['title']
        except KeyError:
            record['title'] = result['attributes']['name']

        exists = scraperwiki.sql.select(
            "* FROM '" + table + "' WHERE id=?", record['id'])
        if len(exists) == 0:
            print(record)
            if SLACK_WEBHOOK_URL and SEND_NOTIFICATIONS:
                post_slack_message(record)

            if table in ['onsad', 'onspd']:
                if GITHUB_API_KEY and SEND_NOTIFICATIONS:
                    raise_github_issue('polling_deploy', record)
            if table == 'onspd':
                if GITHUB_API_KEY and SEND_NOTIFICATIONS:
                    raise_github_issue('EveryElection', record)
            if table == 'lgd' and 'full extent' in record['title'].lower():
                if GITHUB_API_KEY and SEND_NOTIFICATIONS:
                    raise_github_issue('UK-Polling-Stations', record)

        scraperwiki.sqlite.save(
            unique_keys=['id'], data=record, table_name=table)
        scraperwiki.sqlite.commit_transactions()


onspd_url = 'http://ons.maps.arcgis.com/sharing/rest/search?q=(tags%3AONS%20Postcode%20Directory%20type%3ACSV%20orgid%3AESMARspQHYMw9BZ9%20orgid%3AESMARspQHYMw9BZ9)%20-type%3A%22Layer%22%20-type%3A%20%22Map%20Document%22%20-type%3A%22Map%20Package%22%20-type%3A%22Basemap%20Package%22%20-type%3A%22Mobile%20Basemap%20Package%22%20-type%3A%22Mobile%20Map%20Package%22%20-type%3A%22ArcPad%20Package%22%20-type%3A%22Project%20Package%22%20-type%3A%22Project%20Template%22%20-type%3A%22Desktop%20Style%22%20-type%3A%22Pro%20Map%22%20-type%3A%22Layout%22%20-type%3A%22Explorer%20Map%22%20-type%3A%22Globe%20Document%22%20-type%3A%22Scene%20Document%22%20-type%3A%22Published%20Map%22%20-type%3A%22Map%20Template%22%20-type%3A%22Windows%20Mobile%20Package%22%20-type%3A%22Layer%20Package%22%20-type%3A%22Explorer%20Layer%22%20-type%3A%22Geoprocessing%20Package%22%20-type%3A%22Desktop%20Application%20Template%22%20-type%3A%22Code%20Sample%22%20-type%3A%22Geoprocessing%20Package%22%20-type%3A%22Geoprocessing%20Sample%22%20-type%3A%22Locator%20Package%22%20-type%3A%22Workflow%20Manager%20Package%22%20-type%3A%22Windows%20Mobile%20Package%22%20-type%3A%22Explorer%20Add%20In%22%20-type%3A%22Desktop%20Add%20In%22%20-type%3A%22File%20Geodatabase%22%20-type%3A%22Feature%20Collection%20Template%22%20-type%3A%22Code%20Attachment%22%20-type%3A%22Featured%20Items%22%20-type%3A%22Symbol%20Set%22%20-type%3A%22Color%20Set%22%20-type%3A%22Windows%20Viewer%20Add%20In%22%20-type%3A%22Windows%20Viewer%20Configuration%22%20&sortField=modified&sortOrder=desc&num=10&f=json'
onsud_url = 'http://ons.maps.arcgis.com/sharing/rest/search?q=(tags%3AONS%20UPRN%20Directory%20type%3ACSV%20orgid%3AESMARspQHYMw9BZ9%20orgid%3AESMARspQHYMw9BZ9)%20-type%3A%22Layer%22%20-type%3A%20%22Map%20Document%22%20-type%3A%22Map%20Package%22%20-type%3A%22Basemap%20Package%22%20-type%3A%22Mobile%20Basemap%20Package%22%20-type%3A%22Mobile%20Map%20Package%22%20-type%3A%22ArcPad%20Package%22%20-type%3A%22Project%20Package%22%20-type%3A%22Project%20Template%22%20-type%3A%22Desktop%20Style%22%20-type%3A%22Pro%20Map%22%20-type%3A%22Layout%22%20-type%3A%22Explorer%20Map%22%20-type%3A%22Globe%20Document%22%20-type%3A%22Scene%20Document%22%20-type%3A%22Published%20Map%22%20-type%3A%22Map%20Template%22%20-type%3A%22Windows%20Mobile%20Package%22%20-type%3A%22Layer%20Package%22%20-type%3A%22Explorer%20Layer%22%20-type%3A%22Geoprocessing%20Package%22%20-type%3A%22Desktop%20Application%20Template%22%20-type%3A%22Code%20Sample%22%20-type%3A%22Geoprocessing%20Package%22%20-type%3A%22Geoprocessing%20Sample%22%20-type%3A%22Locator%20Package%22%20-type%3A%22Workflow%20Manager%20Package%22%20-type%3A%22Windows%20Mobile%20Package%22%20-type%3A%22Explorer%20Add%20In%22%20-type%3A%22Desktop%20Add%20In%22%20-type%3A%22File%20Geodatabase%22%20-type%3A%22Feature%20Collection%20Template%22%20-type%3A%22Code%20Attachment%22%20-type%3A%22Featured%20Items%22%20-type%3A%22Symbol%20Set%22%20-type%3A%22Color%20Set%22%20-type%3A%22Windows%20Viewer%20Add%20In%22%20-type%3A%22Windows%20Viewer%20Configuration%22%20&sortField=modified&sortOrder=desc&num=10&f=json'
lgd_url = 'https://opendata.arcgis.com/api/v2/datasets?filter%5Bcatalogs%5D=geoportal1-ons.opendata.arcgis.com&include=organizations%2Cgroups&page%5Bnumber%5D=1&page%5Bsize%5D=10&q=LGD+Boundaries&sort=-updatedAt'
wards_url = 'https://opendata.arcgis.com/api/v2/datasets?filter%5Bcatalogs%5D=geoportal1-ons.opendata.arcgis.com&include=organizations%2Cgroups&page%5Bnumber%5D=1&page%5Bsize%5D=10&q=WD_NC&sort=-updatedAt'

init()
scrape(onspd_url, 'onspd')
scrape(onsud_url, 'onsad')  # retain old table for backwards-compatibility
scrape(lgd_url, 'lgd')
scrape(wards_url, 'wards')
