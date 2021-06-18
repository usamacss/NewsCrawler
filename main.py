from datetime import datetime
from bs4 import BeautifulSoup
import requests
import configparser
import DB

parser = configparser.ConfigParser()
parser.read("config.ini")


def confParser(section):
    if not parser.has_section(section):
        print("No section info  rmation are available in config file for", section)
        return
    # Build dict
    tmp_dict = {}
    for option, value in parser.items(section):
        option = str(option)
        value = value.encode("utf-8")
        tmp_dict[option] = value
    return tmp_dict


def get_page_obj(url):
    page_obj = None
    try:
        response = requests.get(url, timeout=50)
        if response.status_code == 200:
            page_html = response.text
            page_obj = BeautifulSoup(page_html, 'lxml')
    except Exception as e:
        print(e)
    return page_obj


general_conf = confParser("general_conf")
base_url = general_conf["url"].decode("utf-8")

if __name__ == '__main__':
    if not DB.check_table_exists():
        print("Creating table...")
        DB.create_table()
    page_obj = get_page_obj(base_url)
    if page_obj is not None:
        news_articles = page_obj.findAll("article")
        for article in news_articles:
            try:
                header = article.find("header")
                news_link = header.find("a").attrs["href"]
                title = header.find("a").attrs["title"].replace('"', '').replace("'", "").replace("\n", "").replace(",", "").strip()
                sub_title = header.findAll("span")[0].text.replace('"', '').replace("'", "").replace("\n", "").replace(",", "").strip()
                try:
                    abstract = article.find("section").text.replace('"', '').replace("'", "").replace("\n", "").replace(",", "").strip()
                except:
                    abstract = ""
                download_time = str(datetime.now()).split(".")[0]
                if not DB.check_news_exists(news_link):
                    DB.insert_scraped_data(news_link, title, sub_title, abstract, download_time)
                else:
                    DB.update_scraped_time(news_link, download_time)
            except Exception as e:
                print(e)
        print("download complete...")