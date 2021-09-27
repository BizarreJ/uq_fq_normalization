# Federated Upper Quartile and Quantile Normalization FeatureCloud App

## Description
A Normalization FeatureCloud App, allowing to perform Quantile (Q) or Upper Quartile (UQ) Normalization in a federated manner.

## Config
For an example config file look at the `sampleconfig.yml` in this directory.

Use the config file to customize things. Just upload it together with your data as `config.yml`
```
uq_q_normalization:   
    normalization: upper quartile   #required; normalization method you want to use. All clients should have the same here.
    
    input_filename: client.csv      #optional; name of your input file, don't forget the .csv at the end. Default is data.csv.
    sample_genes_in_input: False    #optional; set this True, if the first row and col of the input matrix are the names of 
                                    genes and samples. Default is False.

    normfactors: False              #optional; if this is True, an extra .csv file with the normalization factors will be created.
                                    Default is False.
    output_filename: res.csv        #optional; name of the output file, don't forget the .csv at the end. Default is result.csv.
    sample_names: col.txt           #optional; if the sample names are not given in the input file, you can specify them here. 
                                    The file must be a .txt file (don't forget the .txt). In "Input" is defined how the file 
                                    should look like. Default is None.
    gene_names: row.txt             #optional; if the gene names are not given in the input file, you can specify them here. 
                                    The file must be a .txt file (don't forget the .txt). In "Input" is defined how the file 
                                    should look like. Default is None.
    
    seperator: ';'                  #optional; specify the seperator you use in the input .csv file. Default is a comma.
```


## Input
* A read count matrix in a .csv file.  
Example:  
        2,4,4,0  
        0,8,6,9  
        3,9,3,5  
        1,3,8,2  
* A `config.yml` file to customize some things (see above)  
* Optional:
  * a .txt file with the names of the samples (the names should be in separate lines).  
    Example:  
        sample1  
        sample2  
        sample3  
        sample4    
  * a .txt file with the names of the genes (the names should be in separate lines).  
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

## How to test it with FeatureCloud
0. You need a Docker Installation on your computer.
1. Log in at https://featurecloud.ai/
2. Make sure, that you enabled the developer mode (*your name -> edit profile -> put a check mark at the bottom of the page*).
3. Download the FeatureCloud Controller from https://featurecloud.ai/getting-started and execute it. You get a new directory named "data". If you now go to the FeatureCloud Website to *For Developers -> Controller*, the Controller status should be "online" and "Docker is available".
4. go in the new directory, clone this git-Repository. Go into the Repository and execute the command *docker build --tag name .* , you can choose any phrase for *name*. Don't forget the . at the end of the command.
5. now you can go on the FeatureCloud Website to *For Developer -> Testing* and create a new test.
6. The image name is the *name* from 4.. Choose 2 clients for the beginning. There is some sample data for testing in this repository. Complete the paths for Client 1 and 2 for example with *uq_q_normalization/sample_data/client1/* and *uq_q_normalization/sample_data/client2/* or whatever your path to your sample data is. The other settings can remain as they are for now.
7. Click start. It will take a few seconds, than you can see the app work. After another few seconds, the status on the top changes to "Finished" and you can download the results of each client.


