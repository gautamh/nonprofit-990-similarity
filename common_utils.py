import sys

import pandas as pd
import numpy as np

from numpy import argmin
from scipy.spatial.distance import cdist
from numpy import log

def nearest_neighbor(data, col_name, col_value, exclude_cols=[], weights=[]):
    entity_index = np.where(data[col_name] == col_value)[0][0]
    if weights == []:
        entity_dist = cdist([data.drop(exclude_cols, axis=1).iloc[entity_index]], data.drop(exclude_cols, axis=1))
    else:
        entity_dist = cdist([data.drop(exclude_cols, axis=1).iloc[entity_index]], data.drop(exclude_cols, axis=1), 'wminkowski', p=2., w=weights)
    entity_dist = [x if x != 0 else sys.maxint for x in entity_dist[0]]
    nearest_neighbor_index = argmin(entity_dist)
    return data.iloc[nearest_neighbor_index]

def load_data(filepath):
    # read in data
    df = pd.read_csv(filepath)
    df = df.rename(columns=lambda x: str(x).replace('{http://www.irs.gov/efile}', ''))

    non_num = []
    for col in df.columns:
        try:
            df[col].astype('float64')
        except ValueError as e:
            non_num.append(col)
    df_num = df.drop(non_num, axis=1)

    # drop highly correlated columns
    corr_matrix = df_num.corr().abs()
    upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(np.bool))
    to_drop = [column for column in upper.columns if any(upper[column] > 0.80)]
    df2 = df.drop(to_drop, axis=1)

    # shift columns so that all data is positive (for log transformation)
    mins = [(col, df2[[col]].min()) for col in df2.columns]
    for col, min_val in mins:
        try:
            df2[col].astype('float64')
        except ValueError as e:
            print(col)
            continue
        if min_val[0] < 1:
            df2[[col]] = df2[[col]].apply(lambda x: (x - min_val[0]) + 1)
            #df2[[col]] = df2[[col]].apply(lambda x: log((x - min_val[0]) + 1))

    LOG_COLUMNS = [u'GrossReceiptsAmt',
                   u'VotingMembersGoverningBodyCnt',
                   u'VotingMembersIndependentCnt',
                   u'TotalEmployeeCnt',
                   u'CYContributionsGrantsAmt',
                   u'CYGrantsAndSimilarPaidAmt',
                   u'CYBenefitsPaidToMembersAmt',
                   u'CYSalariesCompEmpBnftPaidAmt',
                   u'CYTotalFundraisingExpenseAmt',
                   u'IRPDocumentCnt',
                   u'IRPDocumentW2GCnt',
                   u'CYProgramServiceRevenueAmt',
                   u'CYTotalProfFndrsngExpnsAmt',
                   u'TotalAssetsEOYAmt',
                   u'TotalGrossUBIAmt',
                   u'CYOtherRevenueAmt',
                   u'FormationYr']

    # apply log transformation
    df2[LOG_COLUMNS] = df2[LOG_COLUMNS].apply(lambda x: log(x))
    
    return df2
