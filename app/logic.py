import jsonpickle
import pandas as pd
import threading
import time
import yaml
import numpy as np

from app.algo import Coordinator, Client
from distutils import dir_util

APP_NAME = 'uq_q_normalization'


class AppLogic:

    def __init__(self):
        # === Status of this app instance ===
        # Indicates whether there is data to share, if True make sure self.data_out is available
        self.status_available = False

        # Only relevant for coordinator, will stop execution when True
        self.status_finished = False

        # === Parameters set during setup ===
        self.id = None
        self.coordinator = None
        self.clients = None

        # === Data ===
        self.data_incoming = []
        self.data_outgoing = None

        # === Internals ===
        self.thread = None
        self.iteration = 0
        self.progress = 'not started yet'

        # === Custom ===
        self.INPUT_DIR = "/mnt/input/"
        self.OUTPUT_DIR = "/mnt/output/"

        self.client = None
        self.mode = None
        self.input_name = None
        self.sample_names = None
        self.gene_names = None
        self.sep = None
        self.output_normfac = False
        self.output_name = None

        self.samples = None
        self.genes = None
        self.colsrows = False

    # This method is called once upon startup and contains information about the execution context of this instance
    def handle_setup(self, client_id, coordinator, clients):
        self.id = client_id
        self.coordinator = coordinator
        self.clients = clients
        print(f'Received setup: {self.id} {self.coordinator} {self.clients}', flush=True)

        self.read_config()

        self.thread = threading.Thread(target=self.app_flow)
        self.thread.start()
        

    def handle_incoming(self, data):
        # This method is called when new data arrives
        print("Process incoming data....", flush=True)
        self.data_incoming.append(data.read())

    def handle_outgoing(self):
        print("Process outgoing data...", flush=True)
        # This method is called when data is requested
        self.status_available = False
        return self.data_outgoing

    def read_config(self):
        dir_util.copy_tree("/mnt/input/", "/mnt/output/")
        with open("/mnt/input/config.yml") as f:
            config = yaml.load(f, Loader=yaml.FullLoader)[APP_NAME]

            self.mode = config.get("normalization")

            self.input_name = config.get("input_filename", "data.csv")  
            self.colsrows = config.get("sample_genes_in_input", False)

            self.output_normfac = config.get("normfactors", False)
            self.output_name = config.get("output_filename", "result.csv")
            self.sample_names = config.get("sample_names", None)
            self.gene_names = config.get("gene_names", None)

            self.sep = config.get("seperator", ",")

    def app_flow(self):
        # This method contains a state machine for the client and coordinator instance
        # === States ===
        state_initializing = 1
        state_read_input = 2
        state_local_computation = 3
        state_local_result_computation = 4
        state_wait_for_aggregation = 5
        state_global_aggregation = 6
        state_second_wait_for_aggregation = 7
        state_global_result_computation = 8
        state_set_local_result = 9
        state_writing_results = 10
        state_finishing = 11

        # Initial state
        state = state_initializing
        self.progress = 'initializing...'

        printcols = False
        printrows = False
        if self.sample_names is not None:
            colpath = f"{self.INPUT_DIR}{self.sample_names}"
            with open(colpath,"r") as tf:
                self.samples = tf.read().splitlines()
            printcols = True
        if self.gene_names is not None:
            rowpath = f"{self.INPUT_DIR}{self.gene_names}"
            with open(rowpath,"r") as tf:
                self.genes = tf.read().splitlines()
            printrows = True
        if (self.colsrows):
            printcols = True
            printrows = True

        while True:
            if state == state_initializing:
                print("Initializing", flush=True)
                if self.id is not None:  # Test if setup has happened already
                    print(f'Coordinator: {self.coordinator}', flush=True)
                    if self.coordinator:
                        self.client = Coordinator()
                    else:
                        self.client = Client()
                    state = state_read_input
            if state == state_read_input:
                print("Read input", flush=True)
                self.progress = 'read input'
                self.client.read_input(self.input_name,self.sep,self.samples,self.genes,self.colsrows)
                state = state_local_computation

            if state == state_local_computation:
                self.progress = 'local computation'
                if self.mode == "quantile":
                    print("Start Quantile Normalization")
                    print("Local mean computation", flush=True)
                    self.client.q_compute_local_means()

                    data_to_send = jsonpickle.encode(self.client.local_means)
                elif self.mode == "upper quartile":
                    print("Start Upper Quartile Normalization")
                    print("Local zero computation", flush=True)
                    self.client.uq_compute_local_zeros()
                
                    data_to_send = jsonpickle.encode(self.client.local_zeros)
                else:
                    print("ERROR: there was no normalization method given in config.yml")
                    exit()

                if self.coordinator:
                    self.data_incoming.append(data_to_send)
                    state = state_global_aggregation
                else:
                    self.data_outgoing = data_to_send
                    self.status_available = True
                    state = state_wait_for_aggregation
                    if self.mode == "quantile":
                        print(f'[CLIENT] Sending local means data to coordinator', flush=True)
                    elif self.mode == "upper quartile":
                        print(f'[CLIENT] Sending local zero lines to coordinator', flush=True)

            if state == state_wait_for_aggregation:
                print("Wait for aggregation", flush=True)
                self.progress = 'wait for aggregation'
                if len(self.data_incoming) > 0:
                    if self.mode == "quantile":
                        print("Received global means from coordinator.", flush=True)
                        global_means = jsonpickle.decode(self.data_incoming[0])
                        self.client.q_set_global_means(global_means)
                    elif self.mode == "upper quartile":
                        print("Received global zero lines from coordinator.", flush=True)
                        global_zeros = jsonpickle.decode(self.data_incoming[0])
                        self.client.uq_set_global_zeros(global_zeros)
                    self.data_incoming = []
                    state = state_local_result_computation

            if state == state_global_aggregation:
                print("Global computation", flush=True)
                self.progress = 'global aggregation...'
                if len(self.data_incoming) == len(self.clients):
                    if self.mode == "quantile":
                        local_means = [jsonpickle.decode(client_data) for client_data in self.data_incoming]             
                        global_means = self.client.q_compute_global_means(local_means)
                        self.client.q_set_global_means(global_means)
                        data_to_broadcast = jsonpickle.encode(global_means)
                    elif self.mode == "upper quartile":
                        local_zeros = [jsonpickle.decode(client_data) for client_data in self.data_incoming]
                        global_zeros = self.client.uq_compute_global_zeros(local_zeros)
                        self.client.uq_set_global_zeros(global_zeros)
                        data_to_broadcast = jsonpickle.encode(global_zeros)
                    self.data_incoming = []
                    self.data_outgoing = data_to_broadcast
                    self.status_available = True
                    state = state_local_result_computation
                    if self.mode == "quantile":
                        print(f'[COORDINATOR] Broadcasting global mean to clients', flush=True)
                    elif self.mode == "upper quartile":
                        print(f'[COORDINATOR] Broadcasting global zero lines to clients', flush=True)

            if state == state_local_result_computation:
                if self.mode == "quantile":
                    print("Calculating results..", flush=True)
                    self.client.q_compute_local_result()
                    state = state_writing_results
                elif self.mode == "upper quartile":
                    print("Calculating local norm factors..", flush=True)
                    self.client.uq_compute_uquartile()
                    data_to_send = jsonpickle.encode(self.client.uquartile)

                    if self.coordinator:
                        self.data_incoming.append(data_to_send)
                        state = state_global_result_computation
                    else:
                        self.data_outgoing = data_to_send
                        self.status_available = True
                        state = state_second_wait_for_aggregation
                        print(f'[CLIENT] Sending local norm factors to coordinator', flush=True)

            if state == state_second_wait_for_aggregation:
                print("Wait for the second aggregation", flush=True)
                self.progress = 'wait for second aggregation'
                if len(self.data_incoming) > 0:
                    print("Received global result from coordinator.", flush=True)
                    global_result = jsonpickle.decode(self.data_incoming[0])
                    self.data_incoming = []
                    self.client.uq_set_global_result(global_result)
                    state = state_set_local_result

            if state == state_global_result_computation:
                print("Global computation of the result", flush=True)
                self.progress = 'global result computation...'
                if len(self.data_incoming) == len(self.clients):
                    local_result = []
                    for client_data in self.data_incoming:
                        local_result = np.append(local_result,jsonpickle.decode(client_data))
                    self.data_incoming = []
                    global_result = self.client.uq_compute_global_result(local_result)
                    self.client.uq_set_global_result(global_result)
                    data_to_broadcast = jsonpickle.encode(global_result)
                    self.data_outgoing = data_to_broadcast
                    self.status_available = True
                    state = state_set_local_result
                    print(f'[COORDINATOR] Broadcasting global result to clients', flush=True)

            if state == state_set_local_result:
                print("Calculating results..", flush=True)
                self.client.uq_compute_local_result()
                state = state_writing_results

            if state == state_writing_results:
                print("Writing results", flush=True)
                # now you can save it to a file
                self.client.write_results(self.output_name, printcols, printrows)
                if self.mode == "upper quartile" and self.output_normfac:
                    self.client.write_normfac("normfactor.csv", self.samples)
                state = state_finishing

            if state == state_finishing:
                print("Finishing", flush=True)
                self.progress = 'finishing...'
                if self.coordinator:
                    time.sleep(10)
                self.status_finished = True
                break
            
            time.sleep(1)

logic = AppLogic()
