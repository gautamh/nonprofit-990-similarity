import time

import pandas as pd
import numpy as np
from ggplot import *
from sklearn.manifold import TSNE

import common_utils

df = common_utils.load_data('C:/Users/Gautam/nonprofit_features_ntee.csv')

df2 = df.loc[df['NTEE_CD'] != 'Z00']

feature_cols = [u'GrossReceiptsAmt', u'VotingMembersGoverningBodyCnt', u'VotingMembersIndependentCnt', u'TotalEmployeeCnt', u'CYContributionsGrantsAmt', u'CYGrantsAndSimilarPaidAmt', u'CYBenefitsPaidToMembersAmt', u'CYSalariesCompEmpBnftPaidAmt', u'CYTotalFundraisingExpenseAmt', u'IRPDocumentCnt', u'IRPDocumentW2GCnt', u'CYProgramServiceRevenueAmt', u'CYTotalProfFndrsngExpnsAmt', u'TotalAssetsEOYAmt', u'TotalGrossUBIAmt', u'CYOtherRevenueAmt', u'FormationYr']

labels = df2["NTEE_CD"].astype('category').cat.codes.values
features = df2[list(feature_cols)].values
df2["NTEE_CD"] = df2["NTEE_CD"].astype('str').str[0]

labels = labels[np.isfinite(features).all(axis=1)]
features = features[np.isfinite(features).all(axis=1)]

print("t-SNE starting")
time_start = time.time()
tsne = TSNE(n_components=2, verbose=1, perplexity=20, n_iter=300)
tsne_results = tsne.fit_transform(df2.loc[:,feature_cols].values)

print('t-SNE done! Time elapsed: {} seconds'.format(time.time()-time_start))

df_tsne = df2.copy()
df_tsne['x-tsne'] = tsne_results[:,0]
df_tsne['y-tsne'] = tsne_results[:,1]

chart = ggplot( df_tsne, aes(x='x-tsne', y='y-tsne', color='NTEE_CD') ) \
        + geom_point(size=6,alpha=0.15) \
        + ggtitle("tSNE dimensions colored by digit")
chart.show()