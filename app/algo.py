import sys, math, csv
import pandas as pd
import numpy as np
from functools import reduce
from scipy.stats import rankdata

INPUT_PATH = "/mnt/input/data.csv"
OUTPUT_PATH = "/mnt/output/result.txt"

class Client:
    input_data = None
    local_means = None
    global_means = None
    result = None

    def __init__(self):
        pass

    def read_input(self):
        try:
            self.input_data = pd.read_csv(INPUT_PATH, header=None)
        except FileNotFoundError:
            print(f'File {INPUT_PATH} could not be found.', flush=True)
            exit()
        except Exception as e:
            print(f'File could not be parsed: {e}', flush=True)
            exit()

    def compute_local_means(self):
        data_sorted = np.sort(self.input_data,axis=0)
        print(data_sorted)
        self.local_means = np.mean(data_sorted,axis=1)
        print(f'Local means: {self.local_means}', flush=True)

    def compute_local_result(self):
        def rank_to_mean(value):
            if value%1 == 0:
                return self.global_means[int(value)-1]
            else:
                return (self.global_means[math.ceil(value)-1] + self.global_means[math.floor(value)-1])/2

        #cols = list(self.input_data.columns)
        #rows = list(self.input_data.index)

        ranks = rankdata(self.input_data, axis=0, method='average')
        print(ranks)
        print(type(ranks), type(self.global_means))
        self.result = pd.DataFrame(ranks).applymap(rank_to_mean)

    def set_global_means(self, global_means):
        self.global_means = global_means
        print(f'Global means: {self.global_means}', flush=True)

    def write_results(self):
        f = open(OUTPUT_PATH, "a")
        f.write(str(self.result))
        f.close()


class Coordinator(Client):
    def compute_global_means(self, local_means):
        return np.sum(local_means,axis=0)/len(local_means)
