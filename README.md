# Federated UQ and Q Normalization FeatureCloud App

## Description

## Input

## Output

## Workflows

## How to test it with FeatureCloud
0. You need a Docker Installation on your computer.
1. Log in at https://featurecloud.ai/
2. Make sure, that you enabled the developer mode (*your name -> edit profile -> put a check mark at the bottom of the page*).
3. Download the FeatureCloud Controller from https://featurecloud.ai/getting-started and execute it. You get a new directory named "data". If you now go to the FeatureCloud Website to *For Developers -> Controller*, the Controller status should be "online" and "Docker is available".
4. go in the new directory, clone this git-Repository. Go into the Repository and execute the command *docker build --tag name .* , you can choose any phrase for *name*. Don't forget the . at the end of the command.
5. now you can go on the FeatureCloud Website to *For Developer -> Testing* and create a new test. 
6. The image name is the *name* from 4.. Choose 2 clients for the beginning. There is some sample data for testing in this repository. Complete the paths for Client 1 and 2 for example with *uq_q_normalization/sample_data/client1/* and *uq_q_normalization/sample_data/client2/* or whatever your path to your sample data is. The other settings can remain as they are for now.
7. Click start. It will take a few seconds, than you can see the app work. After another few seconds, the status on the top changes to "Finished" and you can download the results of each client.
