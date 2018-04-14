import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.cross_validation import cross_val_score

import common_utils

df = common_utils.load_data('C:/Users/Gautam/nonprofit_features_ntee.csv')

df2 = df.loc[df['NTEE_CD'] != 'Z00']

feature_cols = [u'GrossReceiptsAmt', u'VotingMembersGoverningBodyCnt', u'VotingMembersIndependentCnt', u'TotalEmployeeCnt', u'CYContributionsGrantsAmt', u'CYGrantsAndSimilarPaidAmt', u'CYBenefitsPaidToMembersAmt', u'CYSalariesCompEmpBnftPaidAmt', u'CYTotalFundraisingExpenseAmt', u'IRPDocumentCnt', u'IRPDocumentW2GCnt', u'CYProgramServiceRevenueAmt', u'CYTotalProfFndrsngExpnsAmt', u'TotalAssetsEOYAmt', u'TotalGrossUBIAmt', u'CYOtherRevenueAmt', u'FormationYr']

labels = df2["NTEE_CD"].astype('category').cat.codes.values
features = df2[list(feature_cols)].values

labels = labels[np.isfinite(features).all(axis=1)]
features = features[np.isfinite(features).all(axis=1)]

rf = RandomForestClassifier(max_depth=2, random_state=0)

rf.fit(features, labels)

weights = rf.feature_importances_

print(common_utils.nearest_neighbor(df, 'EIN', 133871360, ['EIN', 'NTEE_CD'], weights))
print(common_utils.nearest_neighbor(df, 'EIN', 133871360, ['EIN', 'NTEE_CD']))