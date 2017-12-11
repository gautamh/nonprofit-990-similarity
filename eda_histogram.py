import sys

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def nearest_neighbor(data, col_name, col_value, exclude_cols=[]):
    entity_index = np.where(data[col_name] == col_value)[0][0]
    entity_dist = cdist([data.drop(exclude_cols, axis=1).iloc[entity_index]], data.drop(exclude_cols, axis=1))
    entity_dist = [x if x != 0 else sys.maxint for x in entity_dist[0]]
    nearest_neighbor_index = argmin(entity_dist)
    return data.iloc[nearest_neighbor_index]


# read in data
df = pd.read_csv('C:/Users/Gautam/nonprofit_features.csv')
df = df.rename(columns=lambda x: str(x).replace('{http://www.irs.gov/efile}', ''))

# drop highly correlated columns
corr_matrix = df.corr().abs()
upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(np.bool))
to_drop = [column for column in upper.columns if any(upper[column] > 0.80)]
df2 = df.drop(to_drop, axis=1)

# shift columns so that all data is positive (for log transformation)
mins = [(col, df2[[col]].min()) for col in df2.columns]
for col, min_val in mins:
    if min_val[0] < 1:
        df2[[col]] = df2[[col]].apply(lambda x: x - min_val[0] + 1)

LOG_COLUMNS = [u'GrossReceiptsAmt', u'VotingMembersGoverningBodyCnt', u'VotingMembersIndependentCnt', u'TotalEmployeeCnt', u'CYContributionsGrantsAmt', u'CYGrantsAndSimilarPaidAmt', u'CYBenefitsPaidToMembersAmt', u'CYSalariesCompEmpBnftPaidAmt', u'CYTotalFundraisingExpenseAmt', u'IRPDocumentCnt', u'IRPDocumentW2GCnt', u'CYProgramServiceRevenueAmt', u'CYTotalProfFndrsngExpnsAmt', u'TotalAssetsEOYAmt', u'TotalGrossUBIAmt']

# apply log transformation
df2[LOG_COLUMNS] = df2[LOG_COLUMNS].apply(lambda x: np.log(x))

# create histogram...
df2.hist(layout=(3,6), bins=50)

# ...and voila!
plt.show()