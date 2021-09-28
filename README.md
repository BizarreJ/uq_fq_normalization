# Federated Upper Quartile and Quantile Normalization FeatureCloud App

## Description
A Normalization FeatureCloud App, allowing to perform Quantile (Q) or Upper Quartile (UQ) Normalization in a federated manner.
The app produces results very similar to those of `calcNormFactors(input_matrix, method = "upperquartile")` in the Bioconductor edgeR package of Bullard et al. [0] and `normalizeBetweenArrays(input_matrix, method="quantile")` in the Bioconductor limma package of Bolstat et al. [1].  
Note: the upper quartile method cannot handle NaNs in the input data. if NaNs are present in the input data, the quantile method should be used.

[0] Bullard JH, Purdom E, Hansen KD, Dudoit S. (2010) Evaluation of statistical methods for normalization and differential expression in mRNA-Seq experiments. BMC Bioinformatics 11, 94.  
[1] Bolstad, B. M., Irizarry R. A., Astrand, M., and Speed, T. P. (2003), A comparison of normalization methods for high density oligonucleotide array data based on bias and variance. Bioinformatics 19, 185-193. 

## Config
For an example config file look at the `sampleconfig.yml` in this directory.

Use the config file to customize things. Just upload it together with your data as `config.yml`
```
uq_q_normalization:   
    normalization: upper quartile   #required; normalization method you want to use. 
                                    All clients should have the same here.
    
    input_filename: client.csv      #optional; name of your input file, don't forget 
                                    the .csv at the end. Default is data.csv.
    sample_genes_in_input: False    #optional; set this True, if the first row and col of the 
                                    input matrix are the names of genes and samples. Default is False.

    normfactors: False              #optional; if this is True, an extra .csv file with the 
                                    normalization factors will be created. Default is False.
    output_filename: res.csv        #optional; name of the output file, don't forget the .csv 
                                    at the end. Default is result.csv.
    sample_names: col.txt           #optional; if the sample names are not given in the input file, 
                                    you can specify them here. The file must be a .txt file (don't forget the .txt). 
                                    In "Input" is defined how the file should look like. Default is None.
    gene_names: row.txt             #optional; if the gene names are not given in the input file, 
                                    you can specify them here. The file must be a .txt file (don't forget the .txt). 
                                    In "Input" is defined how the file should look like. Default is None.
    
    seperator: ';'                  #optional; specify the seperator you use in the input .csv file. 
                                    Default is a comma.
```


## Input
* A read count matrix in a .csv file.  
Example:  
        2,4,4,0  
        0,8,6,9  
        3,9,3,5  
        1,3,8,2 
or  
        ,sample1,sample2,sample3,sample4
        gene1,2,4,4,0
        gene2,0,8,6,9
        gene3,3,9,3,5
        gene4,1,3,8,2
* A `config.yml` file to customize some things (see above)  
* Optional:
  * a .txt file with the names of the samples, if the sample names are not in the input.csv (the names should be in separate lines).  
    Example:  
        sample1  
        sample2  
        sample3  
        sample4    
  * a .txt file with the names of the genes, if the gene names are not in the input.csv (the names should be in separate lines).  
    Example:  
        gene1  
        gene2  
        gene3  
        gene4  

## Output
* A matrix with normalized read counts.  
* Optional:
  * A file with the normalization factors, if defined in `config.yml` (only available for upper quartile normalization).

## Workflows
This is a standalone App at the moment.

## Open ToDos
* make sure that the normalization method only has to be defined in the `config.yml` file of the coordinator
* implement drop down menu to select the method


