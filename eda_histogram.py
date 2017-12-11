import sys

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import common_utils

df2 = common_utils.load_data('C:/Users/Gautam/nonprofit_features.csv')

# create histogram...
df2.hist(layout=(3,6), bins=50)

# ...and voila!
plt.show()