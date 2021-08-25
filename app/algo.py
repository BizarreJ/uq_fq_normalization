import sys, math, csv
import pandas as pd
import numpy as np
import scipy
from functools import reduce
from scipy.stats import rankdata
from scipy import interpolate

INPUT_PATH = "/mnt/input/data.csv"
OUTPUT_PATH = "/mnt/output/result.txt"

class Client:
    input_data = None

    local_means = None
    global_means = None
    arr = None
    m = None
    n = None
    nobs = None
    i = None

    local_zeros = None
    global_zeros = None
    client_id = None
    uqfactor = None
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

#-------------------------------------------------------------------------
# Quartile Implementation:

    #Prepares the calculation of the quantiles. calculates the mean vector of the client.
    #The implementation is based on the implementation of the 
    #normalizeBetweenArrays method in bioconductor limma. 
    #Gordon and Smyth, 2005
    def q_compute_local_means(self):
        if(isinstance(self.input_data, pd.DataFrame)):
            data = self.input_data.to_numpy()
        
        try:
            self.n,self.m = data.shape
        except ValueError:
            print("Error in Quantile function: The input matrix has too few rows or columns.")
            exit()
          
        self.arr = np.zeros((self.n,self.m))
        for i in range(self.m):
            self.arr[:,i] = data[:,i].astype(np.float64)
            
        if self.n == 1:
            mean = np.mean(self.arr)
            return np.array(self.m * [mean])
        if self.m == 1:
            return self.arr
        
        Ix = np.empty((self.n,self.m))
        Ix[:] = np.nan
        Sort = Ix.copy()
        
        nobs = np.array(self.m * [self.n])
        i = np.arange(self.n)/(self.n-1)
        
        for j in range(self.m):
            six = np.sort(self.arr[:,j])
            x = self.arr[:,j]
            x = x[~(np.isnan(x))]
            temp = x.argsort()
            siix = np.empty_like(temp)
            siix[temp] = np.arange(len(x))
            
            nobsj = six.size - np.count_nonzero(np.isnan(six))
            if nobsj < self.n:
                nobs[j] = nobsj
                six = six[~(np.isnan(six))]
                isna = np.isnan(self.arr[:,j])
                f = scipy.interpolate.interp1d((np.arange(nobsj)/(nobsj-1)), six)
                Sort[:,j] = f(i)
                Ix[~isna,j] = ((np.arange(self.n))[~isna])[siix]
            else:
                Sort[:,j] = six
                Ix[:,j] = siix

        self.nobs = nobs
        self.i = i

        self.local_means = np.mean(Sort, axis=1)
        print(f'Local means vector: {self.local_means}', flush=True)

    #Calculates the result of the normalization.
    def q_compute_local_result(self):
        for j in range(self.m):
            r = rankdata(self.arr[:,j], method='average')
            if(self.nobs[j] < self.n):
                isna = np.isnan(self.arr[:,j])
                f = scipy.interpolate.interp1d(self.i, self.global_means)
                self.arr[~isna,j] = f((r[~isna]-1)/(self.nobs[j]-1))
            else:
                f = scipy.interpolate.interp1d(self.i, self.global_means)
                self.arr[:,j] = f((r-1)/(self.n-1))

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
        if n == 1:
            print("Error in Upper Quartile function: There are too few lines left after removing the zeros.")
            exit()

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
        try:
            global_means = np.sum(local_means,axis=0)/len(local_means)
        except ValueError:
            print("Error in Quantile function: The input matrices of all clients must have the same number of rows.")
            exit()   
        return global_means
    
    #Collects the zero lines of the clients and 
    #reduces them to the lines that are present in each client.
    def uq_compute_global_zeros(self, local_zeros):
        return reduce(np.intersect1d, local_zeros)
    
    #Calculates the global result
    def uq_compute_global_result(self,total_dict):
        keys = list(total_dict.keys())
        result = []
        for k in keys:
            result.append(total_dict.get(k))
        result = result/(np.exp(np.mean(np.log(result))))
        for i in range(len(total_dict)):
            total_dict[keys[i]] = result[i]
        return total_dict





