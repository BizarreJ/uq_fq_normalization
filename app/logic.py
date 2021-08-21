import jsonpickle
import pandas as pd
import threading
import time
import yaml

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
        self.INPUT_DIR = "/mnt/input"
        self.OUTPUT_DIR = "/mnt/output"

        self.client = None
        self.mode = None

    def handle_setup(self, client_id, coordinator, clients):
        # This method is called once upon startup and contains information about the execution context of this instance
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
        state_writing_results = 9
        state_finishing = 10

        # Initial state
        state = state_initializing
        self.progress = 'initializing...'

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
                self.client.read_input()
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
                print("Calculating results..", flush=True)
                if self.mode == "quantile":
                    self.client.q_compute_local_result()
                    state = state_writing_results
                elif self.mode == "upper quartile":
                    self.client.uq_compute_local_result()
                    data_to_send = jsonpickle.encode(self.client.local_result)

                    if self.coordinator:
                        self.data_incoming.append(data_to_send)
                        state = state_global_result_computation
                    else:
                        self.data_outgoing = data_to_send
                        self.status_available = True
                        state = state_second_wait_for_aggregation
                        print(f'[CLIENT] Sending local result to coordinator', flush=True)

            if state == state_second_wait_for_aggregation:
                print("Wait for another aggregation", flush=True)
                self.progress = 'wait for second aggregation'
                if len(self.data_incoming) > 0:
                    print("Received global result from coordinator.", flush=True)
                    global_result = jsonpickle.decode(self.data_incoming[0])
                    self.data_incoming = []
                    self.client.uq_set_global_result(global_result)
                    state = state_writing_results

            if state == state_global_result_computation:
                print("Global computation of the result", flush=True)
                self.progress = 'global result computation...'
                if len(self.data_incoming) == len(self.clients):
                    local_result = [jsonpickle.decode(client_data) for client_data in self.data_incoming]
                    self.data_incoming = []
                    global_result = self.client.uq_compute_global_result(local_result)
                    self.client.uq_set_global_result(global_result)
                    data_to_broadcast = jsonpickle.encode(global_result)
                    self.data_outgoing = data_to_broadcast
                    self.status_available = True
                    state = state_writing_results
                    print(f'[COORDINATOR] Broadcasting global result to clients', flush=True)

            if state == state_writing_results:
                print("Writing results", flush=True)
                # now you can save it to a file
                self.client.write_results()
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
