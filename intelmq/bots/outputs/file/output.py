# -*- coding: utf-8 -*-
import io
import os

from intelmq.lib.bot import Bot


class FileOutputBot(Bot):

    def init(self):
        self.logger.debug("Opening %r file.", self.parameters.file)
        self.file = io.open(self.parameters.file, mode='at', encoding="utf-8")
        self.logger.info("File %r is open.", self.parameters.file)

    def process(self):
        event = self.receive_message()
        event_data = event.to_json(hierarchical=self.parameters.hierarchical_output)

        if not os.fstat(self.file.fileno()).st_nlink:
            self.logger.info('File has been deleted, opening again.')
            self.init()
        try:
            self.file.write(event_data)
            self.file.write("\n")
            self.file.flush()
        except FileNotFoundError:
            self.init()
        else:
            self.acknowledge_message()

    def shutdown(self):
        self.file.close()


BOT = FileOutputBot
