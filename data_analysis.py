import pandas as pd
import warnings
import numpy as np
import seaborn as sns
import sqlite3
from scipy.stats import stats
from scipy.stats import ttest_ind


warnings.filterwarnings("ignore")
df = pd.read_csv("data.csv")
