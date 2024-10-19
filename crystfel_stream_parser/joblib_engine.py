import regex
import signal
import sys
import joblib
import copy

from more_itertools import divide

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
            'geometry'            : regex.compile( r"(?s)----- Begin geometry file -----\n(?P<GEOM_BLOCK>.*?)\n----- End geometry file -----" ),

            'chunk'               : regex.compile( r"(?s)----- Begin chunk -----\n(?P<CHUNK_BLOCK>.*?)\n----- End chunk -----" ),
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


    def parse_one_geom(self, geom_block_content):
        kv_pattern_dict = self.kv_pattern_dict

        # Extract key-value pairs (represented by both : and =)...
        geom_kv_record = {}
        for delimiter, kv_pattern in kv_pattern_dict.items():
            kv_pattern = kv_pattern_dict[delimiter]
            for kv_match in regex.finditer(kv_pattern, geom_block_content):
                capture_dict = kv_match.capturesdict()

                k = capture_dict['LEFT'][0]
                v = capture_dict['RIGHT'][0]

                geom_kv_record[k] = v

        return geom_kv_record


    def parse_one_block(self, stream_block_content):
        chunk_block = stream_block_content['CHUNK_BLOCK']
        geom_block  = stream_block_content['GEOM_BLOCK' ]

        chunk_record = self.parse_one_chunk(chunk_block[0]) if len(chunk_block) > 0 else {}
        geom_record  = self.parse_one_geom ( geom_block[0]) if len( geom_block) > 0 else {}

        record = { 'GEOM_BLOCK' : geom_record,
                   'CHUNK_BLOCK': chunk_record, }

        return record


    def parse(self, num_cpus=2, returns_stream_dict=False):
        # Shutdown joblib during a Ctrl+C event...
        def signal_handler(sig, frame):
            print('SIGINT (Ctrl+C) caught, shutting down...')
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)

        # Unpack attributes...
        path_stream        = self.path_stream
        block_pattern_dict = self.block_pattern_dict
        kv_pattern_dict    = self.kv_pattern_dict
        peak_pattern_dict  = self.peak_pattern_dict

        # Define the work load for each worker...
        def parse_blocks(blocks):
            '''
            Arguments:
                - blocks: a list of [(block_idx, block_content),...].
            '''
            stream_record_dict = {}
            for block_idx, block_content in blocks:
                stream_record = self.parse_one_block(block_content)
                stream_record_dict[block_idx] = stream_record
            return stream_record_dict

        with open(path_stream, 'r') as fh:
            data = fh.read()

        # Match either a chunk block or a geom block...
        chunk_or_geom = [f"({block_pattern_dict['chunk'].pattern})", f"({block_pattern_dict['geometry'].pattern})"]
        chunk_or_geom_pattern = '|'.join(chunk_or_geom)
        chunk_or_geom_block_content_list = [(block_idx, match.capturesdict()) for block_idx, match in enumerate(regex.finditer(chunk_or_geom_pattern, data))]

        block_batches = divide(num_cpus, chunk_or_geom_block_content_list)

        # Use joblib for parallel processing
        results = joblib.Parallel(n_jobs=num_cpus)(
            joblib.delayed(parse_blocks)(blocks) for blocks in block_batches
        )

        stream_record_dict = {}
        for result in results:
            stream_record_dict.update(result)

        # Prune the stream graph to only save chunks...
        chunk_record_idx  = 0
        chunk_record_dict = {}
        valid_geom_block  = None
        uses_copy         = True
        is_ref            = True
        for block_idx in sorted(stream_record_dict.keys()):
            record = stream_record_dict[block_idx]

            # Cache the geom block???
            if len(record['GEOM_BLOCK']) > 0:
                valid_geom_block = record['GEOM_BLOCK']
                uses_copy = True
                is_ref    = True

            # Save the chunk directly following a geom block saves the raw and functions as a reference...
            else:
                if uses_copy:
                    valid_geom_block = copy.deepcopy(valid_geom_block)
                    uses_copy = False
                record['GEOM_BLOCK' ] = valid_geom_block
                record['IS_REF_GEOM_BLOCK'] = is_ref
                if is_ref: is_ref = False

                chunk_record_dict[chunk_record_idx] = record
                chunk_record_idx += 1

        return stream_record_dict if returns_stream_dict else chunk_record_dict
