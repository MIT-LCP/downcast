from messages import NumericValueMessage

class NumericValueHandler:
    def __init__(self, archive):
        self.archive = archive
        self.files = set()

    def send_message(self, chn, msg, source, ttl):
        if not isinstance(msg, NumericValueMessage):
            return

        source.nack_message(chn, msg, self)

        # Load metadata for this numeric
        attr = msg.origin.get_numeric_attr(msg.numeric_id, (ttl <= 0))
        if attr is None:
            # Metadata not yet available - hold message in pending and
            # continue processing
            return

        # Look up the corresponding record and add event to the time map
        record = self.archive.get_record(msg)

        # Open or create a log file
        logfile = record.open_log_file('_numerics')
        self.files.add(logfile)

        # Write value to the log file
        time = msg.sequence_number - record.seqnum0()
        val = msg.value
        logfile.append('%d,%s,%s' % (time, attr.sub_label, val))
        source.ack_message(chn, msg, self)

    def flush(self):
        for f in self.files:
            f.flush()
        self.files = set()
        self.archive.flush()
