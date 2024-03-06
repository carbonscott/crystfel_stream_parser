import regex
import signal
import sys
import ray

from .utils import split_list_into_chunk

class StreamParser:

    def __init__(self, path_stream):
        self.path_stream = path_stream

        self.block_pattern_dict = None
        self.kv_pattern_dict    = None
        self.peak_pattern_dict  = None
        self.init_regex()


    def init_regex(self):
        # Macro regex to parse blocks...
        block_pattern_dict = {
            'geometry'            : regex.compile( r"(?s)----- Begin geometry file -----\n(?P<BLOCK>.*?)\n----- End geometry file -----" ),

            'chunk'               : regex.compile( r"(?s)----- Begin chunk -----\n(?P<BLOCK>.*?)\n----- End chunk -----" ),
            'found peak meta'     : regex.compile( r"(?s)(?P<BLOCK>.*?)Peaks from peak search" ),
            'found peak'          : regex.compile( r"(?s)Peaks from peak search\n(?P<BLOCK>.*?)\nEnd of peak list" ),

            'crystal'             : regex.compile( r"(?s)--- Begin crystal\n(?P<BLOCK>.*?)\n--- End crystal" ),
            'predicted peak meta' : regex.compile( r"(?s)(?P<BLOCK>.*?)\nReflections measured after indexing" ),
            'predicted peak'      : regex.compile( r"(?s)Reflections measured after indexing\n(?P<BLOCK>.*?)\nEnd of reflections" ),
        }

        # Micro regex...
        # ...key-value pattern
        kv_pattern_dict = {
            '=' : regex.compile(
                    r"""(?x)
                        (?&LEFT) \s = \s (?&RIGHT)

                        (?(DEFINE)
                            (?<LEFT>
                                [^=:\n]+?
                            )
                            (?<RIGHT>
                                (?> ([^=:\n]+) )
                            )
                        )
                    """
            ),

            ':' : regex.compile(
                    r"""(?x)
                        (?&LEFT) : \s (?&RIGHT)

                        (?(DEFINE)
                            (?<LEFT>
                                [^:=\n]+?
                            )
                            (?<RIGHT>
                                (?> [^:=\n]+ )
                            )
                        )
                    """
            ),
        }

        # ...Peak pattern
        # Found peak:   536.50  377.50       4.83     1583.80   p0a0
        # Predicted peak:  -29   38   10      55.28     928.47      47.30      39.77   69.1 1230.4 p0a0
        peak_pattern_dict = {
            "found peak" : regex.compile(r"""(?x)  # Process input line by line
                (?>
                    (?:
                        (?>
                            (?&FLOAT) # Match a floating number
                        )
                        \s+           # Match whitespace at least once
                    ){4}              # Match the whole construct 4 times
                )

                (?&DET_PANEL)         # Match a detector panel

                (?(DEFINE)
                    (?<FLOAT>
                        [-+]?         # Match a sign
                        (?> \d+ )       # Match integer part
                        (?: \.\d* )?    # Match decimal part
                    )
                    (?<DET_PANEL>
                        (?: [0-9A-Za-z]+ )
                    )
                )
                """
            ),

            "predicted peak" : regex.compile(r"""(?x)
                (?:
                    (?>
                        (?&FLOAT) # Match a floating number
                    )
                    \s+           # Match whitespace at least once
                ){9}              # Match the whole construct 4 times

                (?&DET_PANEL)     # Match a detector panel

                (?(DEFINE)
                    (?<FLOAT>
                        [-+]?         # Match a sign
                        (?>\d+)       # Match integer part
                        (?:\.\d*)?    # Match decimal part
                    )
                    (?<DET_PANEL>
                        (?: [0-9A-Za-z]+ )
                    )
                )
                """
            ),
        }

        self.block_pattern_dict = block_pattern_dict
        self.kv_pattern_dict    = kv_pattern_dict
        self.peak_pattern_dict  = peak_pattern_dict


    def parse_one_chunk(self, chunk_block_content):
        block_pattern_dict = self.block_pattern_dict
        kv_pattern_dict    = self.kv_pattern_dict
        peak_pattern_dict  = self.peak_pattern_dict

        # Enter 'found peak meta' block...
        chunk_kv_record = {}
        match = regex.search(block_pattern_dict['found peak meta'], chunk_block_content)
        if match is not None:
            # Extract key-value pairs (represented by both : and =)...
            capture_dict = match.capturesdict()
            content = capture_dict['BLOCK'][0]
            for delimiter, kv_pattern in kv_pattern_dict.items():
                kv_pattern = kv_pattern_dict[delimiter]
                for kv_match in regex.finditer(kv_pattern, content):
                    capture_dict = kv_match.capturesdict()

                    k = capture_dict['LEFT'][0]
                    v = capture_dict['RIGHT'][0]

                    chunk_kv_record[k] = v

        # Enter 'found peak' block...
        found_peak_dict = {}
        match = regex.search(block_pattern_dict['found peak'], chunk_block_content)
        if match is not None:
            # Extract peak info...
            capture_dict = match.capturesdict()
            content = capture_dict['BLOCK'][0]
            for match in regex.finditer(peak_pattern_dict['found peak'], content):
                capture_dict = match.capturesdict()
                pixel_info   = capture_dict['FLOAT']
                pixel_info   = [ float(info) for info in pixel_info ]
                det_panel    = capture_dict['DET_PANEL'][0]
                if det_panel not in found_peak_dict: found_peak_dict[det_panel] = []
                found_peak_dict[det_panel].append(pixel_info)

        # Enter 'crystal' block...
        crystal_list = []
        for match in regex.finditer(block_pattern_dict['crystal'], chunk_block_content):
            capture_dict = match.capturesdict()
            crystal_block_content = capture_dict['BLOCK'][0]

            # Enter 'predicted peak meta' block...
            kv_record = {}
            match = regex.search(block_pattern_dict['predicted peak meta'], crystal_block_content)
            if match is not None:
                # Extract key-value pairs (represented by both : and =)...
                capture_dict = match.capturesdict()
                content = capture_dict['BLOCK'][0]
                for delimiter, kv_pattern in kv_pattern_dict.items():
                    kv_pattern = kv_pattern_dict[delimiter]
                    for kv_match in regex.finditer(kv_pattern, content):
                        capture_dict = kv_match.capturesdict()

                        k = capture_dict['LEFT'][0]
                        v = capture_dict['RIGHT'][0]

                        kv_record[k] = v

            # Enter 'predicted peak' block...
            predicted_peak_dict = {}
            match = regex.search(block_pattern_dict['predicted peak'], crystal_block_content)
            if match is not None:
                # Extract peak info...
                capture_dict = match.capturesdict()
                content = capture_dict['BLOCK'][0]
                for match in regex.finditer(peak_pattern_dict['predicted peak'], content):
                    capture_dict = match.capturesdict()
                    pixel_info   = capture_dict['FLOAT']
                    pixel_info   = [ float(info) for info in pixel_info ]
                    det_panel    = capture_dict['DET_PANEL'][0]
                    if det_panel not in predicted_peak_dict: predicted_peak_dict[det_panel] = []
                    predicted_peak_dict[det_panel].append(pixel_info)

            crystal_list.append({
                'metadata'        : kv_record,
                'predicted peaks' : predicted_peak_dict,
            })


        stream_record = {
            'metadata'    : chunk_kv_record,
            'found peaks' : found_peak_dict,
            'crystal'     : crystal_list,
        }

        return stream_record


    def parse(self, num_cpus = 2):
        # Shutdown ray clients during a Ctrl+C event...
        def signal_handler(sig, frame):
            if ray.is_initialized():
                print('SIGINT (Ctrl+C) caught, shutting down Ray...')
                ray.shutdown()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)

        # Init ray...
        ray.init(num_cpus = num_cpus)

        # Unpack attributes...
        path_stream        = self.path_stream
        block_pattern_dict = self.block_pattern_dict
        kv_pattern_dict    = self.kv_pattern_dict
        peak_pattern_dict  = self.peak_pattern_dict

        # Define the work load for each worker...
        @ray.remote
        def parse_chunks(chunks):
            stream_record_list = []
            for chunk in chunks:
                stream_record = self.parse_one_chunk(chunk)
                stream_record_list.append(stream_record)
            return stream_record_list

        with open(path_stream,'r') as fh:
            data = fh.read()

        chunk_block_content_list = [ match.capturesdict()['BLOCK'][0] for match in regex.finditer(block_pattern_dict['chunk'], data) ]
        chunk_block_batches = split_list_into_chunk(chunk_block_content_list, max_num_chunk = num_cpus)

        # Register the computation at remote nodes...
        futures = [parse_chunks.remote(batch) for batch in chunk_block_batches]

        ## # Collect results...
        ## stream_record_list = ray.get(futures)

        stream_record_list = []
        remaining_futures = futures
        while remaining_futures:
            # Wait for at least one task to be ready
            ready_futures, remaining_futures = ray.wait(remaining_futures, num_returns=1, timeout=None)

            # Fetch the result of the ready task(s)
            for future in ready_futures:
                stream_record_list_per_worker = ray.get(future)
                stream_record_list.extend(stream_record_list_per_worker)

        ray.shutdown()

        return stream_record_list
