import os, sys, time
import asyncio
import aiofiles
import random
import csv

import xml.etree.ElementTree as etree

import glob

form_990_path_regex = "/mnt/c/Users/Gautam/form_990/*.xml"
outfile = "/mnt/c/Users/Gautam/nonprofit_features.csv"

EXTRACTABLE_FEATURE_FIELDS_STR = """{http://www.irs.gov/efile}GrossReceiptsAmt,
{http://www.irs.gov/efile}VotingMembersGoverningBodyCnt,
{http://www.irs.gov/efile}VotingMembersIndependentCnt,
{http://www.irs.gov/efile}TotalEmployeeCnt,
{http://www.irs.gov/efile}CYContributionsGrantsAmt,
{http://www.irs.gov/efile}CYGrantsAndSimilarPaidAmt,
{http://www.irs.gov/efile}CYBenefitsPaidToMembersAmt,
{http://www.irs.gov/efile}CYSalariesCompEmpBnftPaidAmt,
{http://www.irs.gov/efile}CYTotalFundraisingExpenseAmt,
{http://www.irs.gov/efile}IRPDocumentCnt,
{http://www.irs.gov/efile}IRPDocumentW2GCnt,
{http://www.irs.gov/efile}EmployeeCnt,
{http://www.irs.gov/efile}GoverningBodyVotingMembersCnt,
{http://www.irs.gov/efile}IndependentVotingMemberCnt,
{http://www.irs.gov/efile}CYProgramServiceRevenueAmt,
{http://www.irs.gov/efile}CYTotalProfFndrsngExpnsAmt,
{http://www.irs.gov/efile}CYTotalExpensesAmt,
{http://www.irs.gov/efile}TotalAssetsEOYAmt,
{http://www.irs.gov/efile}CYOtherExpensesAmt,
{http://www.irs.gov/efile}TotalLiabilitiesEOYAmt,
{http://www.irs.gov/efile}CYTotalRevenueAmt,
{http://www.irs.gov/efile}TotalGrossUBIAmt,
{http://www.irs.gov/efile}TotalAssetsBOYAmt,
{http://www.irs.gov/efile}PYTotalExpensesAmt,
{http://www.irs.gov/efile}PYTotalRevenueAmt,
{http://www.irs.gov/efile}PYOtherExpensesAmt,
{http://www.irs.gov/efile}CYInvestmentIncomeAmt,
{http://www.irs.gov/efile}NetAssetsOrFundBalancesEOYAmt,
{http://www.irs.gov/efile}CYOtherRevenueAmt,
{http://www.irs.gov/efile}NetAssetsOrFundBalancesBOYAmt,
{http://www.irs.gov/efile}FormationYr,
{http://www.irs.gov/efile}PYContributionsGrantsAmt,
{http://www.irs.gov/efile}TotalProgramServiceExpensesAmt"""

EXTRACTABLE_FEATURE_FIELDS = EXTRACTABLE_FEATURE_FIELDS_STR.replace("\n", "").split(",")

counter = 0
num_fields_counts = {}
outlist = []

#for filename in glob.glob(form_990_path_regex):
#    with open(filename, 'r') as f:
#        x = f.read()
#        counter += 1
#        if counter % 1000 == 0:
#            print(counter)

def get_numfields_from_filing(filing):
    num_fields = []
    try:
        root = etree.fromstring(filing)
        form_990 = root.find("./{http://www.irs.gov/efile}ReturnData/{http://www.irs.gov/efile}IRS990")
        if (form_990 is not None and len(form_990) > 0):
            for child in form_990:
                if child.text is not None and child.text.isdigit() and not child.tag.endswith('Ind'):
                    num_fields.append(child.tag)
    except etree.ParseError as e:
        print(str(e))
    return num_fields

def get_num_features_from_filing(filing):
    num_field_features = []
    try:
        root = etree.fromstring(filing)
        ein = root.find("./{http://www.irs.gov/efile}ReturnHeader/{http://www.irs.gov/efile}Filer/{http://www.irs.gov/efile}EIN").text
        form_990 = root.find("./{http://www.irs.gov/efile}ReturnData/{http://www.irs.gov/efile}IRS990")
        if (form_990 is not None and len(form_990) > 0):
            num_field_features.append(ein)
            for feature_field_tag in EXTRACTABLE_FEATURE_FIELDS:
                feature_field = form_990.find(feature_field_tag)
                if feature_field is not None:
                    num_field_features.append(feature_field.text)
                else:
                    num_field_features.append(str(-1))
    except etree.ParseError as e:
        print(str(e))
    return num_field_features

def process_fields_to_counts(tags, counts_dict):
    for tag in tags:
        if tag in counts_dict:
            counts_dict[tag] += 1
        else:
            counts_dict[tag] = 1

def process_filing_features(features, outlist):
    if len(features) > 0:
        outlist.append(features)

async def produce(queue, filename):
    contents = ''
    async with aiofiles.open(filename, mode='r') as f:
        contents = await f.read()
    await queue.put(get_num_features_from_filing(contents))

async def consume(queue):
    global counter
    global num_fields_counts
    global outlist
    while True:
        # wait for an item from the producer
        processed_filing_result = await queue.get()
        # process_fields_to_counts(processed_filing_result, num_fields_counts)
        process_filing_features(processed_filing_result, outlist)
        counter += 1

        # Notify the queue that the item has been processed
        queue.task_done()

async def run(n):
    queue = asyncio.Queue()
    # schedule the consumer
    consumer = asyncio.ensure_future(consume(queue))
    # run the producer and wait for completion
    for filename in glob.glob(form_990_path_regex):
        await produce(queue, filename)
    # wait until the consumer has processed all items
    await queue.join()
    # the consumer is still awaiting for an item, cancel it
    consumer.cancel()
    print(counter)
    #s = [(k, num_fields_counts[k]) for k in sorted(num_fields_counts, key=num_fields_counts.get, reverse=True)]
    #for k,v in s:
    #    print('{}, {}'.format(k, v))
    with open(outfile, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(["EIN"] + EXTRACTABLE_FEATURE_FIELDS)
        writer.writerows(outlist)
    print("done")


loop = asyncio.get_event_loop()
loop.run_until_complete(run(10))
loop.close()