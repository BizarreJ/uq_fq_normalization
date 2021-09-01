import sys, math, csv
import pandas as pd
import numpy as np
import scipy
from functools import reduce
from scipy.stats import rankdata
from scipy import interpolate

INPUT_PATH = "/mnt/input/"
OUTPUT_PATH = "/mnt/output/result.csv"

class Client:
    input_data = None

    local_means = None
    global_means = None
    arr = None
    nobs = None

    local_zeros = None
    global_zeros = None
    client_id = None
    uqfactor = None
    global_result = None

    result = None

    def __init__(self):
        pass

    def read_input(self, input_name):
        input_path = f"{INPUT_PATH}{input_name}"
        try:
            self.input_data = pd.read_csv(input_path, header=None)
        except FileNotFoundError:
            print(f'File {input_path} could not be found.', flush=True)
            exit()
        except Exception as e:
            print(f'File could not be parsed: {e}', flush=True)
            exit()

    def write_results(self):
        self.result.to_csv(OUTPUT_PATH, header=False, index=False)

#-------------------------------------------------------------------------
# Quartile Implementation:

    #Prepares the calculation of the quantiles. calculates the mean vector of the client.
    #The implementation is based on the implementation of the 
    #normalizeBetweenArrays method in bioconductor limma. 
    #Gordon and Smyth, 2005
    def q_compute_local_means(self):
        if(isinstance(self.input_data, pd.DataFrame)):
            data = self.input_data.to_numpy()
        
        n, m = data.shape
          
        self.arr = np.zeros((n,m))
        for i in range(m):
            self.arr[:,i] = data[:,i].astype(np.float64)
            
        if n == 1:
            mean = np.mean(self.arr)
            self.local_means = np.array(m * [mean])
            return
        if m == 1:
            self.local_means = self.arr
            return
        
        Sort = np.empty((n,m))
        Sort[:] = np.nan
        
        nobs = np.array(m * [n])
        i = np.arange(n)/(n-1)
        
        for j in range(m):
            col = np.sort(self.arr[:,j])
            
            nobsj = len(col) - np.count_nonzero(np.isnan(col))
            if nobsj < n:
                nobs[j] = nobsj
                col = col[~(np.isnan(col))]
                f = scipy.interpolate.interp1d((np.arange(nobsj)/(nobsj-1)), col)
                Sort[:,j] = f(i)
            else:
                Sort[:,j] = col

        self.nobs = nobs

        self.local_means = [m, np.sum(Sort, axis=1)]
        print(f'Local means vector: {self.local_means}', flush=True)

    #Calculates the result of the normalization.
    def q_compute_local_result(self):
        n,m = self.arr.shape
        i = np.arange(n)/(n-1)

        for j in range(m):
            r = rankdata(self.arr[:,j], method='average')
            f = scipy.interpolate.interp1d(i, self.global_means)
            if(self.nobs[j] < n):
                isna = np.isnan(self.arr[:,j])
                self.arr[~isna,j] = f((r[~isna]-1)/(self.nobs[j]-1))
            else:
                self.arr[:,j] = f((r-1)/(n-1))

        self.result = pd.DataFrame(self.arr)

    #Set the global means vector.
    def q_set_global_means(self, global_means):
        self.global_means = global_means
        print(f'Global means vector: {self.global_means}', flush=True)

#---------------------------------------------------------------------------
# Upper Quartile Implementation
    
    #Checks which lines of the client's input_data are completely zero.
    def uq_compute_local_zeros(self):
        if(self.input_data.isnull().values.any()):
            print("Error in Upper Quartile function: the function can't handle NaNs in input data.", flush=True)
            exit()
        all_zero = self.input_data.eq(0).all(axis=1)
        self.local_zeros = np.where(all_zero)[0]
        print(f'Local zeros of this client in lines: {self.local_zeros}', flush=True)

    #Calculates for each sample of the client the upper quartile by library size factor (uqfactor).
    #The implementation is based on the implementation of the 
    #calcNormFactors method in bioconductor edgeR. 
    #Robinson and Smyth, 2020
    def uq_compute_uqfactor(self, client_id):
        self.input_data.drop(axis=1, index=self.global_zeros, inplace=True)
        
        n,m = self.input_data.shape
        if n== 0 or n == 1:
            self.uqfactor = np.array(m * [1])
            return
        if m == 1:
            self.uqfactor = 1
            return

        data = pd.DataFrame(np.sort(self.input_data,axis=0))
        self.client_id = client_id
    
        lib_size = np.array(data.sum(axis=0).tolist())
        uquartile = np.quantile(data,0.75,axis=0)
        self.uqfactor = {client_id: uquartile/lib_size}
        print(f'Local result: {self.uqfactor}', flush=True)

    #Compute the local result.
    def uq_compute_local_result(self):
        norm_factors = self.global_result[self.client_id]
        self.result = self.input_data/norm_factors

    #Set the global zeros vector.
    def uq_set_global_zeros(self, global_zeros):
        self.global_zeros = global_zeros
        print(f'Global zeros in lines: {self.global_zeros}', flush=True)

    #Set the global Norm Factor Vector.
    def uq_set_global_result(self, global_result):
        self.global_result = global_result
        print(f'Global result: {self.global_result}', flush=True)


class Coordinator(Client):
    #Aggregates the mean values of the clients.
    def q_compute_global_means(self, local_means):
        np.warnings.filterwarnings('ignore', category=np.VisibleDeprecationWarning)
        local_means = np.sum(local_means,axis=0)
        return local_means[1]/local_means[0]
        
    
    #Collects the zero lines of the clients and 
    #reduces them to the lines that are present in each client.
    def uq_compute_global_zeros(self, local_zeros):
        return reduce(np.intersect1d, local_zeros)
    
    #Calculates the global result
    def uq_compute_global_result(self,total_dict):
        np.warnings.filterwarnings('ignore', category=np.VisibleDeprecationWarning)
        keys = list(total_dict.keys())
        result = []
        nosamples = []
        for k in keys:
            result = np.concatenate((result, total_dict.get(k)),axis=None)
            nosamples.append(len(total_dict.get(k)))
        result = result/(np.exp(np.mean(np.log(result))))
        n = 0
        for i in range(len(total_dict)):
            local_dict = []
            for j in range(n,n + nosamples[i]):
                local_dict.append(result[j])
            total_dict[keys[i]] = local_dict
            n += nosamples[i]
        return total_dict





