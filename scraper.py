import os
import requests
from polling_bot.brain import SlackClient, GitHubClient
from sqlalchemy.exc import OperationalError

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
            record['title'] = result['attributes']['title']

        try:
            exists = scraperwiki.sql.select(
                "* FROM '" + table + "' WHERE id=?", record['id'])
            if len(exists) == 0:
                print(record)
                if SLACK_WEBHOOK_URL and SEND_NOTIFICATIONS:
                    post_slack_message(record)
                if table in ['onsad', 'onspd']:
                    if GITHUB_API_KEY and SEND_NOTIFICATIONS:
                        raise_github_issue('polling_deploy', record)
                if table == 'lgd':
                    if GITHUB_API_KEY and SEND_NOTIFICATIONS:
                        raise_github_issue('UK-Polling-Stations', record)
        except OperationalError:
            # The first time we run the scraper it will throw
            # because the table doesn't exist yet
            pass

        scraperwiki.sqlite.save(
            unique_keys=['id'], data=record, table_name=table)
        scraperwiki.sqlite.commit_transactions()


onspd_url = 'http://ons.maps.arcgis.com/sharing/rest/search?q=(tags%3AONS%20Postcode%20Directory%20type%3ACSV%20orgid%3AESMARspQHYMw9BZ9%20orgid%3AESMARspQHYMw9BZ9)%20-type%3A%22Layer%22%20-type%3A%20%22Map%20Document%22%20-type%3A%22Map%20Package%22%20-type%3A%22Basemap%20Package%22%20-type%3A%22Mobile%20Basemap%20Package%22%20-type%3A%22Mobile%20Map%20Package%22%20-type%3A%22ArcPad%20Package%22%20-type%3A%22Project%20Package%22%20-type%3A%22Project%20Template%22%20-type%3A%22Desktop%20Style%22%20-type%3A%22Pro%20Map%22%20-type%3A%22Layout%22%20-type%3A%22Explorer%20Map%22%20-type%3A%22Globe%20Document%22%20-type%3A%22Scene%20Document%22%20-type%3A%22Published%20Map%22%20-type%3A%22Map%20Template%22%20-type%3A%22Windows%20Mobile%20Package%22%20-type%3A%22Layer%20Package%22%20-type%3A%22Explorer%20Layer%22%20-type%3A%22Geoprocessing%20Package%22%20-type%3A%22Desktop%20Application%20Template%22%20-type%3A%22Code%20Sample%22%20-type%3A%22Geoprocessing%20Package%22%20-type%3A%22Geoprocessing%20Sample%22%20-type%3A%22Locator%20Package%22%20-type%3A%22Workflow%20Manager%20Package%22%20-type%3A%22Windows%20Mobile%20Package%22%20-type%3A%22Explorer%20Add%20In%22%20-type%3A%22Desktop%20Add%20In%22%20-type%3A%22File%20Geodatabase%22%20-type%3A%22Feature%20Collection%20Template%22%20-type%3A%22Code%20Attachment%22%20-type%3A%22Featured%20Items%22%20-type%3A%22Symbol%20Set%22%20-type%3A%22Color%20Set%22%20-type%3A%22Windows%20Viewer%20Add%20In%22%20-type%3A%22Windows%20Viewer%20Configuration%22%20&sortField=modified&sortOrder=desc&num=10&f=json'
onsud_url = 'http://ons.maps.arcgis.com/sharing/rest/search?q=(tags%3AONS%20UPRN%20Directory%20type%3ACSV%20orgid%3AESMARspQHYMw9BZ9%20orgid%3AESMARspQHYMw9BZ9)%20-type%3A%22Layer%22%20-type%3A%20%22Map%20Document%22%20-type%3A%22Map%20Package%22%20-type%3A%22Basemap%20Package%22%20-type%3A%22Mobile%20Basemap%20Package%22%20-type%3A%22Mobile%20Map%20Package%22%20-type%3A%22ArcPad%20Package%22%20-type%3A%22Project%20Package%22%20-type%3A%22Project%20Template%22%20-type%3A%22Desktop%20Style%22%20-type%3A%22Pro%20Map%22%20-type%3A%22Layout%22%20-type%3A%22Explorer%20Map%22%20-type%3A%22Globe%20Document%22%20-type%3A%22Scene%20Document%22%20-type%3A%22Published%20Map%22%20-type%3A%22Map%20Template%22%20-type%3A%22Windows%20Mobile%20Package%22%20-type%3A%22Layer%20Package%22%20-type%3A%22Explorer%20Layer%22%20-type%3A%22Geoprocessing%20Package%22%20-type%3A%22Desktop%20Application%20Template%22%20-type%3A%22Code%20Sample%22%20-type%3A%22Geoprocessing%20Package%22%20-type%3A%22Geoprocessing%20Sample%22%20-type%3A%22Locator%20Package%22%20-type%3A%22Workflow%20Manager%20Package%22%20-type%3A%22Windows%20Mobile%20Package%22%20-type%3A%22Explorer%20Add%20In%22%20-type%3A%22Desktop%20Add%20In%22%20-type%3A%22File%20Geodatabase%22%20-type%3A%22Feature%20Collection%20Template%22%20-type%3A%22Code%20Attachment%22%20-type%3A%22Featured%20Items%22%20-type%3A%22Symbol%20Set%22%20-type%3A%22Color%20Set%22%20-type%3A%22Windows%20Viewer%20Add%20In%22%20-type%3A%22Windows%20Viewer%20Configuration%22%20&sortField=modified&sortOrder=desc&num=10&f=json'
lgd_url = 'https://opendata.arcgis.com/api/v2/datasets?filter%5Bcatalogs%5D=geoportal1-ons.opendata.arcgis.com&include=organizations%2Cgroups&page%5Bnumber%5D=1&page%5Bsize%5D=10&q=LGD+Boundaries&sort=-updatedAt'

scrape(onspd_url, 'onspd')
scrape(onsud_url, 'onsad')  # retain old table for backwards-compatibility
scrape(lgd_url, 'lgd')
