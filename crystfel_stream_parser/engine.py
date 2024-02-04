import os
import regex


class Parser:

    def __init__(self, path_stream):
        self.path_stream = path_stream

        self.marker_dict = {
            "CHUNK_START"      : r"----- Begin chunk -----",
            "CHUNK_END"        : r"----- End chunk -----",
            "GEOM_START"       : r"----- Begin geometry file -----",
            "GEOM_END"         : r"----- End geometry file -----",
            "PEAK_LIST_START"  : r"Peaks from peak search",
            "PEAK_LIST_END"    : r"End of peak list",
            "CRYSTAL_START"    : r"--- Begin crystal",
            "CRYSTAL_END"      : r"--- End crystal",
            "REFLECTION_START" : r"Reflections measured after indexing",
            "REFLECTION_END"   : r"End of reflections",
        }

        self.regex_dict = self.init_regex()

        # Keep results in stream_dict...
        self.stream_dict = {}


    def init_regex(self):
        # Macro regex to parse blocks...
        block_pattern_dict = {
            'geometry'       : regex.compile( r"(?s)----- Begin geometry file -----\n(?P<BLOCK>.*?)\n----- End geometry file -----" ),
            'chunk'          : regex.compile( r"(?s)----- Begin chunk -----\n(?P<BLOCK>.*?)\n----- End chunk -----" ),
            'crystal'        : regex.compile( r"(?s)--- Begin crystal\n(?P<BLOCK>.*?)\n--- End crystal" ),
            'found peak'     : regex.compile( r"(?s)Peaks from peak search\n(?P<BLOCK>.*?)\nEnd of peak list" ),
            'predicted peak' : regex.compile( r"(?s)Reflections measured after indexing\n(?P<BLOCK>.*?)\nEnd of reflections" ),
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

        ## # Parse detector gemoetry...
        ## regex_dict['geom'] = regex.compile(
        ##     r"""
        ##     (?x)
        ##     # Match the pattern below
        ##     (?> (?&DET_PANEL) / ) (?&COORD)
        ##     \s = \s    # Match a equal sign with blank spaces on both sides
        ##     (?&VALUE)  # Match the value of the coordinate

        ##     (?(DEFINE)
        ##         (?<DET_PANEL>
        ##             [0-9a-zA-Z]+
        ##         )
        ##         (?<COORD>
        ##             (?:min_fs)
        ##         |   (?:min_ss)
        ##         |   (?:max_fs)
        ##         |   (?:max_ss)
        ##         )
        ##         (?<VALUE> [0-9]+ $)
        ##     )
        ##     """
        ## )

        return block_pattern_dict, kv_pattern_dict, peak_pattern_dict
