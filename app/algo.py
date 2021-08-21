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
    local_zeros = None
    global_zeros = None
    local_result = None
    global_result = None
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

    def write_results(self):
        f = open(OUTPUT_PATH, "a")
        f.write(str(self.result))
        f.close()

    def q_compute_local_means(self):
        data_sorted = np.sort(self.input_data,axis=0)
        self.local_means = np.mean(data_sorted,axis=1)
        print(f'Local means vector: {self.local_means}', flush=True)

    def q_compute_local_result(self):
        def rank_to_mean(value):
            if value%1 == 0:
                return self.global_means[int(value)-1]
            else:
                return (self.global_means[math.ceil(value)-1] + self.global_means[math.floor(value)-1])/2

        ranks = rankdata(self.input_data, axis=0, method='average')
        self.result = pd.DataFrame(ranks).applymap(rank_to_mean)

    def q_set_global_means(self, global_means):
        self.global_means = global_means
        print(f'Global means vector: {self.global_means}', flush=True)

#---------------------------------------------------------------------------
    
    def uq_compute_local_zeros(self):
        all_zero = self.input_data.eq(0).all(axis=1)
        self.local_zeros = np.where(all_zero)[0]
        print(f'Local zeros of this client in lines: {self.local_zeros}', flush=True)

    def uq_compute_local_result(self):
        self.input_data.drop(axis=1, index=self.global_zeros, inplace=True)
        data = pd.DataFrame(np.sort(self.input_data,axis=0))
    
        lib_size = np.array(data.sum(axis=0).tolist())
        uquartile = np.quantile(data,0.75,axis=0)
        self.local_result = uquartile/lib_size
        print(f'Local result: {self.local_result}', flush=True)

    def uq_set_local_result(self, client_id):
        print("Client ID: ", client_id) #TODO: delete this, its only for debugging
        self.result = self.global_result[int(client_id)]

    def uq_set_global_zeros(self, global_zeros):
        self.global_zeros = global_zeros
        print(f'Global zeros in lines: {self.global_zeros}', flush=True)

    def uq_set_global_result(self, global_result):
        self.global_result = global_result
        print(f'Global result: {self.global_result}', flush=True)


class Coordinator(Client):
    def q_compute_global_means(self, local_means):
        return np.sum(local_means,axis=0)/len(local_means)

    def uq_compute_global_zeros(self, local_zeros):
        return reduce(np.intersect1d, local_zeros)
    
    def uq_compute_global_result(self,local_result):
        global_result = local_result/(np.exp(np.mean(np.log(local_result))))
        return global_result
