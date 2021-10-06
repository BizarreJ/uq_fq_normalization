import sys, math, csv
import pandas as pd
import numpy as np
import scipy
from functools import reduce
from scipy.stats import rankdata
from scipy import interpolate

INPUT_PATH = "/mnt/input/"
OUTPUT_PATH = "/mnt/output/"

class Client:
    input_data = None
    sample_names = None
    gene_names = None

    local_means = None
    global_means = None
    arr = None
    nobs = None

    local_zeros = None
    global_zeros = None
    uquartile = None
    scalingfactor = None
    normfac = None

    result = None

    def __init__(self):
        pass

    def read_input(self, input_name, sep, sample_names = None, gene_names = None, sample_genes_in_input = False):
        input_path = f"{INPUT_PATH}{input_name}"
        try:
            if (sample_genes_in_input):
                self.input_data = pd.read_csv(input_path, sep=sep, index_col = 0)
            else:
                self.input_data = pd.read_csv(input_path, sep=sep, header=None)
        except FileNotFoundError:
            print(f'ERROR: File {input_path} could not be found.', flush=True)
            exit()
        except Exception as e:
            print(f'ERROR: File could not be parsed: {e}', flush=True)
            exit()
        if sample_names is not None:
            self.input_data.columns = sample_names
        if gene_names is not None:        
            self.input_data.index = gene_names
        if(sample_genes_in_input):
            sample_names = list(self.input_data.columns)
            gene_names = list(self.input_data.index)
        self.sample_names = sample_names
        self.gene_names = gene_names

    def write_results(self,output_name,col=False,row=False):
        output_path = f"{OUTPUT_PATH}{output_name}"
        #print(self.result.head(5))
        self.result.to_csv(output_path, header=col, index=row)

    def write_normfac(self,normfac_file, sample_names=None):
        path = f"{OUTPUT_PATH}{normfac_file}"
        if sample_names is not None:
            pd.Series(self.normfac, index=sample_names).to_csv(path, header=False, index=True)
        else:
            pd.Series(self.normfac).to_csv(path, header=False, index=False)

#-------------------------------------------------------------------------
# Quantile Implementation:

    #Prepares the calculation of the quantiles. calculates the mean vector of the client.
    #The implementation is based on the implementation of the 
    #normalizeBetweenArrays method in bioconductor limma. 
    #Gordon and Smyth, 2005
    def q_compute_local_means(self):
        if(isinstance(self.input_data, pd.DataFrame)):
            data = self.input_data.to_numpy()
        
        n, m = self.input_data.shape
          
        self.arr = np.zeros((n,m))
        for i in range(m):
            self.arr[:,i] = data[:,i].astype(np.float64)
         
        if m == 1:
            print("ERROR in Quantile function: There must be more than one sample in the data.", flush=True)
            exit()
        if n == 1:
            if np.any(np.isnan(self.arr)):
                self.arr = self.arr[~np.isnan(self.arr)]
                m = m - np.count_nonzero(np.isnan(self.arr))
                print("WARNING in Quantile function: The samples with NaN values are removed.", flush=True)
            self.local_means = [m, np.sum(self.arr)]
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
        #print(f'Local means vector: {self.local_means}', flush=True)

    #Calculates the result of the normalization.
    def q_compute_local_result(self):
        n,m = self.arr.shape

        if n == 1:
            self.result = pd.DataFrame(np.array(m * [self.global_means])).T
            return
        i = np.arange(n)/(n-1)

        for j in range(m):
            r = rankdata(self.arr[:,j], method='average')
            f = scipy.interpolate.interp1d(i, self.global_means)
            if(self.nobs[j] < n):
                isna = np.isnan(self.arr[:,j])
                self.arr[~isna,j] = f((r[~isna]-1)/(self.nobs[j]-1))
            else:
                self.arr[:,j] = f((r-1)/(n-1))

        self.result = pd.DataFrame(self.arr, index=self.gene_names, columns=self.sample_names)

    #Set the global means vector.
    def q_set_global_means(self, global_means):
        self.global_means = global_means
        #print(f'Global means vector: {self.global_means}', flush=True)

#---------------------------------------------------------------------------
# Upper Quartile Implementation
    
    #Checks which lines of the client's input_data are completely zero.
    def uq_compute_local_zeros(self):
        if(self.input_data.isnull().values.any()):
            print("ERROR in Upper Quartile function: the function can't handle NaNs in input data.", flush=True)
            exit()
        all_zero = self.input_data.eq(0).all(axis=1)
        self.local_zeros = np.where(all_zero)[0]
        #print(f'Local zeros of this client in lines: {self.local_zeros}', flush=True)

    #Calculates for each sample of the client the upper quartile by library size factor (uqfactor).
    #The implementation is based on the implementation of the 
    #calcNormFactors method in bioconductor edgeR. 
    #Robinson and Smyth, 2020
    def uq_compute_uquartile(self):
        indexes_to_keep = set(range(self.input_data.shape[0])) - set(self.global_zeros)
        data = self.input_data.take(list(indexes_to_keep))
        
        n,m = data.shape
        if n == 1:
            print("WARNING in Upper Quartile function: if there is only one gene in matrix, the upper quartile will set to 1.", flush=True)
            self.uquartile = np.array(m * [1])
            return

        data = pd.DataFrame(np.sort(data,axis=0))
    
        self.uquartile = np.quantile(data,0.75,axis=0)
        #print(f'Local result: {self.uquartile}', flush=True)

    #Compute the local result.
    def uq_compute_local_result(self):
        self.normfac = self.uquartile/self.scalingfactor
        self.result = self.input_data/self.normfac

    #Set the global zeros vector.
    def uq_set_global_zeros(self, global_zeros):
        self.global_zeros = global_zeros
        #print(f'Global zeros in lines: {self.global_zeros}', flush=True)

    #Set the global Scaling Factor.
    def uq_set_global_result(self, global_result):
        self.scalingfactor = global_result
        #print(f'Scaling factor: {self.scalingfactor}', flush=True)


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
    def uq_compute_global_result(self,local_uquartile):
        return np.exp(np.mean(np.log(local_uquartile)))





