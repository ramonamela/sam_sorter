

from uuid import uuid4
import os, errno
import shutil
import os
from math import ceil
from multiprocessing import Pool
import pandas as pd
import csv


df = pd.read_csv("/home/ramela/tmp/sam_sorter//da5fcc20-9a92-4eca-a282-fef005a30810/chunk_counter_3.sam", sep="\t", header=None, index_col=False, quoting=csv.QUOTE_NONE)
print(df.shape)