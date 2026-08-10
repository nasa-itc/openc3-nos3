import os
import time
import json

from openc3.api import *
from openc3.api.tlm_api import SUBSCRIPTION_DELIMITER
from openc3.microservices.microservice import Microservice
from openc3.topics.topic import Topic
from openc3.utilities.json import JsonDecoder
from openc3.utilities.sleeper import Sleeper


class CFDP(Microservice):
    def __init__(self, name):
        super().__init__(name)
        self.TARGET_NAME = os.environ.get("CFDP_TARGET_NAME", "CFDP_RADIO")
        # DEBUG and RADIO carry the same downlink telemetry and run as separate
        # microservices. Only one of them may assemble files in the shared
        # /received_files volume, otherwise every data PDU is appended twice.
        self.DOWNLOAD_TARGET_NAME = os.environ.get(
            "CFDP_DOWNLOAD_TARGET_NAME", "CFDP_RADIO"
        )
        self.TLM_PACKET_NAME = "DOWNLINK_FILE_PKT"
        self.CMD_PACKET_NAME = "UPLOAD_TO_SATELLITE_DATA"
        self.period = 2
        self.sleeper = Sleeper()
        self.CHUNK_SIZE = 256
        self.download_path = None
        self.download_sequence = None

    @staticmethod
    def _text(value):
        if isinstance(value, (bytes, bytearray)):
            return bytes(value).split(b"\0", 1)[0].decode("utf-8")
        return str(value).split("\0", 1)[0]

    @staticmethod
    def _block(value):
        if isinstance(value, bytearray):
            return bytes(value)
        if isinstance(value, bytes):
            return value
        if isinstance(value, dict) and value.get("json_class") == "String":
            return bytes(value["raw"])
        # Retained for compatibility with older OpenC3 versions. BLOCK telemetry
        # may arrive as text in those versions.
        if isinstance(value, str):
            return value.encode("latin-1")
        raise TypeError(f"Unsupported OpenC3 BLOCK value: {type(value).__name__}")

    @staticmethod
    def _get_packets(subscription, count=1000):
        """OpenC3 5.12 get_packets with its stream-cursor bug corrected.

        The bundled implementation assigns the input subscription string to
        every updated cursor instead of assigning the Redis message ID yielded
        by Topic.read_topics. That makes its second call fail with an invalid
        stream ID.
        """
        items = subscription.split(SUBSCRIPTION_DELIMITER)
        lookup = dict(zip(items[::2], items[1::2]))
        packets = []
        for topic, message_id, msg_hash, _ in Topic.read_topics(
            lookup.keys(), list(lookup.values()), None, count
        ):
            lookup[topic] = message_id
            msg_hash = {
                key.decode() if isinstance(key, bytes) else key:
                value.decode() if isinstance(value, bytes) else value
                for key, value in msg_hash.items()
            }
            json_hash = json.loads(msg_hash.pop("json_data"), cls=JsonDecoder)
            packets.append(msg_hash | json_hash)

        updated = []
        for topic, message_id in lookup.items():
            updated.extend((topic, message_id))
        return SUBSCRIPTION_DELIMITER.join(updated), packets

    def _check_download_sequence(self, packet):
        sequence = int(packet["SEQUENCE"]) & 0x3FFF
        if self.download_sequence is not None:
            expected = (self.download_sequence + 1) & 0x3FFF
            if sequence != expected:
                self.logger.error(
                    f"CFDP telemetry sequence gap: expected {expected}, got {sequence}"
                )
        self.download_sequence = sequence

    def _receive_file_packet(self, packet):
        self._check_download_sequence(packet)
        destination = self._text(packet["FILENAME_DST"]).strip()
        if not destination:
            self.logger.error("CFDP downlink packet has an empty destination filename")
            return

        path = os.path.join("/received_files", destination)
        pdu = int(packet["PDU_TYPE"])
        if pdu == 2:
            self.logger.info(f"File {destination} downloaded")
            self.download_path = None
            self.download_sequence = None
            return

        size = int(packet["DATA_LENGTH"])
        data = self._block(packet["FILE_DATA"])
        if size < 0 or size > len(data):
            self.logger.error(
                f"Invalid CFDP data length {size}; packet contains {len(data)} bytes"
            )
            return

        # A different destination marks a new transaction. Truncate any old
        # local file on its first data PDU, then append every subsequent PDU.
        mode = "ab" if self.download_path == path else "wb"
        with open(path, mode) as file:
            file.write(data[:size])
        self.download_path = path

    def _send_file(self, packet):
        filename_local = self._text(packet["FILENAME_DST"]).strip()
        filename_remote = self._text(packet["FILENAME_SRC"]).strip()
        path = os.path.join("/send_files", filename_local)

        with open(path, "rb") as file:
            chunk_number = 0
            while True:
                data = file.read(self.CHUNK_SIZE)
                if not data:
                    break
                encoded = data.hex()
                cmd(
                    f"{self.TARGET_NAME} {self.CMD_PACKET_NAME} with "
                    f"LENGTH {len(encoded)}, PDU 1, DESTINATION '{filename_remote}', "
                    f"FILE_DATA '{encoded}'"
                )
                self.logger.info(
                    f"{chunk_number}: sent {len(data)} bytes to spacecraft"
                )
                chunk_number += 1
                time.sleep(0.1)

        cmd(
            f"{self.TARGET_NAME} {self.CMD_PACKET_NAME} with LENGTH 0, "
            f"PDU 2, DESTINATION '{filename_remote}', FILE_DATA ''"
        )
        self.logger.info(f"File {filename_local} uploaded")

    def _process_packet(self, packet):
        direction = int(packet["DIRECTION"])
        if direction == 1:
            if self.TARGET_NAME == self.DOWNLOAD_TARGET_NAME:
                self._receive_file_packet(packet)
        elif direction == 2:
            self._send_file(packet)

    def run(self):
        self.sleeper.sleep(self.period)
        if self.TARGET_NAME == self.DOWNLOAD_TARGET_NAME:
            self.logger.info(f"Receiving CFDP files from {self.TARGET_NAME}")
        else:
            self.logger.info(
                f"Ignoring mirrored CFDP downloads from {self.TARGET_NAME}; "
                f"{self.DOWNLOAD_TARGET_NAME} owns the ground file"
            )
        subscription = subscribe_packets(
            [[self.TARGET_NAME, self.TLM_PACKET_NAME]]
        )
        while not self.cancel_thread:
            # get_packets returns every packet from the Redis stream. Polling tlm()
            # only returns the latest value and loses intermediate file PDUs.
            subscription, packets = self._get_packets(subscription)
            for packet in packets:
                self._process_packet(packet)
            if not packets:
                self.sleeper.sleep(0.1)

    def shutdown(self):
        self.sleeper.cancel()
        super().shutdown()


if __name__ == "__main__":
    CFDP.class_run()
