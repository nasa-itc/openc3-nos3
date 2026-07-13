import os
import time
from openc3.microservices.microservice import Microservice
from openc3.utilities.sleeper import Sleeper
from openc3.api import *


class CFDP(Microservice):
    def __init__(self, name):
        super().__init__(name)
        self.TARGET_NAME = "CFDP"
        self.TLM_PACKET_NAME = "DOWNLINK_FILE_PKT"
        self.CMD_PACKET_NAME = "UPLOAD_TO_SATELLITE_DATA"
        self.period = 2  # 1 second
        self.sleeper = Sleeper()
        self.CHUNK_SIZE = 256
        self.past_data_recv = ""
        self.past_data_send = ""
        self.sending = False
        self.sending_num = 0

    def run(self):
        # Allow the other target processes to start before running the microservice
        self.sleeper.sleep(self.period)
        pastData = ""
        #idMain = subscribe_packets([[self.TARGET_NAME, self.TLM_PACKET_NAME]])
        while True:
            if self.cancel_thread:
                break
            data = tlm(f"{self.TARGET_NAME} {self.TLM_PACKET_NAME} FILE_DATA")
            filename_dst = tlm(f"{self.TARGET_NAME} {self.TLM_PACKET_NAME} FILENAME_DST")
            filename_src = tlm(F"{self.TARGET_NAME} {self.TLM_PACKET_NAME} FILENAME_SRC")
            pdu = tlm(f"{self.TARGET_NAME} {self.TLM_PACKET_NAME} PDU_TYPE")
            direction = tlm(f"{self.TARGET_NAME} {self.TLM_PACKET_NAME} DIRECTION")
            if filename_dst is not None and filename_src is not None:
                if pdu != 2:
                    if direction == 1:
                        if self.past_data_recv != data:
                            if data != "":
                                if type(data) != bytes:
                                    self.logger.info("Data received: {} with type: {}".format(data, type(data)))
                                    with open(os.path.join("/received_files", filename_dst.strip()), "ab") as f:
                                        f.write(data.encode())
                                    self.past_data_recv = data
                                else:
                                    self.logger.info("Length of Data received: {}".format(len(data)))
                                    with open(os.path.join("/received_files", filename_dst.strip()), "ab") as f:
                                        f.write(data)
                                    self.past_data_recv = data
                    if direction == 2:
                        self.sending_num = 0
                        self.sending = True
                        with open(os.path.join("/send_files", filename_dst.strip()), "rb") as file:
                            while self.sending:
                                data = file.read(self.CHUNK_SIZE)
                                if data:
                                    data = data.hex()
                                    bytesRead = len(data)
                                    if self.past_data_send != data:
                                        self.logger.info(f"Sent {self.past_data_send} which is different than {data}")
                                        cmd(f"{self.TARGET_NAME} {self.CMD_PACKET_NAME} with LENGTH {bytesRead}, PDU 1, DESTINATION '{filename_src}', FILE_DATA '{data}'")
                                        self.logger.info(f"{self.sending_num}: Sending {data} to SC")
                                        time.sleep(5)
                                        self.sending_num += 1
                                        self.past_data_send = data
                                else:
                                    self.sending = False
                        self.logger.info(f"Sending EOF to SC")
                        cmd(f"{self.TARGET_NAME} {self.CMD_PACKET_NAME} with LENGTH 0, PDU 2, DESTINATION {filename_src}, FILE_DATA ''")
                        set_tlm(f"{self.TARGET_NAME} {self.TLM_PACKET_NAME} DIRECTION = 99")
                        self.logger.info(f"File {filename_dst} uploaded")
                elif direction != 99:
                    set_tlm(f"{self.TARGET_NAME} {self.TLM_PACKET_NAME} DIRECTION = 99")
                    self.logger.info(f"File {filename_dst} downloaded")

    def shutdown(self):
        self.sleeper.cancel()  # Breaks out of run()
        super().shutdown()

if __name__ == "__main__":
    CFDP.class_run()
