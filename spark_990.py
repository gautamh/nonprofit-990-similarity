from urllib.request import urlopen
from urllib.error import HTTPError
import xml.etree.ElementTree as etree
from operator import add
from pyspark import SparkContext
from http.client import HTTPException
from urllib.error import URLError

sc = SparkContext()

index_file = "/mnt/c/Users/Gautam/Downloads/index_2017.csv"

def process_filing_link(filing_url):
    num_fields = []
    filing = ""
    try:
        with urlopen(filing_url) as resp:
            filing = resp.read()
        root = etree.fromstring(filing)
        form_990 = root.find("./{http://www.irs.gov/efile}ReturnData/{http://www.irs.gov/efile}IRS990")
        if (form_990 is not None and len(form_990) > 0):
            for child in form_990:
                if child.text is not None and child.text.isdigit():
                    num_fields.append((child.tag, 1))
    except HTTPError as e:
        print(str(e))
        print("500 ERROR: {}".format(filing_url))
    except HTTPException as f:
        print(str(e))
        print("HTTPException: {}".format(filing_url))
    except URLError as g:
        print(str(e))
        print("URLError: {}".format(filing_url))
    finally:
        return []
    return num_fields

filings = sc.textFile(index_file)

#split lines
print("splitting lines")
rows = filings.map(lambda line: line.split(','))

#extract header
print("getting header")
header = rows.first()
print("filtering header")
rows = rows.filter(lambda row: row != header) 

#get links
print("getting links")
filing_links = rows.map(lambda row: "https://s3.amazonaws.com/irs-form-990/{}_public.xml".format(row[8]))

#get fields
print("getting fields")
fields = filing_links.flatMap(lambda link: process_filing_link(link))

#sum field counts
print(sorted(fields.reduceByKey(add).collect(), key = lambda x: -x[1]))

    