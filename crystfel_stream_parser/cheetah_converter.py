import regex
import numpy as np


class CheetahConverter:

    def __init__(self, geom_block):
        '''
        A sample geom_block (Dict):
        {
            'q0a0/min_fs': '0',
            'q0a0/min_ss': '0',
            'q0a0/max_fs': '193',
            'q0a0/max_ss': '184',
            'q0a0/fs': '+0.006140x +0.999981y',
            'q0a0/ss': '-0.999981x +0.006140y',
            'q0a0/corner_x': '444.963365',
            'q0a0/corner_y': '-45.414341',
            'q0a0/coffset': '0.5886982000000001',
            'q0a0/no_index': '0',
        }
        '''
        self.geom_block = geom_block

        panel_minmax_pattern = regex.compile(
        r"""(?x)
        # Match the pattern below
        (?> (?&DET_PANEL) / ) (?&COORD)
        \s = \s    # Match a equal sign with blank spaces on both sides
        (?&VALUE)  # Match the value of the coordinate

        (?(DEFINE)
            (?<DET_PANEL>
                [0-9a-zA-Z]+
            )
            (?<COORD>
                (?:min_fs)
            |   (?:min_ss)
            |   (?:max_fs)
            |   (?:max_ss)
            )
            (?<VALUE> [0-9]+)
        )
        """)

        panel_orient_pattern = regex.compile(
            r"""(?x)
            # Match the pattern below
            (?> (?&DET_PANEL) / ) (?&COORD)
            \s = \s    # Match a equal sign with blank spaces on both sides
            (?&VALUE_X) x \s (?&VALUE_Y) y  # Match the value of the coordinate

            (?(DEFINE)
                (?<DET_PANEL>
                    [0-9a-zA-Z]+
                )
                (?<COORD>
                    (?:fs)
                |   (?:ss)
                )
                (?<VALUE_X> 
                    [-+]?         # Match a sign
                    (?>\d+)       # Match integer part
                    (?:\.\d*)?    # Match decimal part
                )
                (?<VALUE_Y> 
                    [-+]?         # Match a sign
                    (?>\d+)       # Match integer part
                    (?:\.\d*)?    # Match decimal part
                )
            )
            """)

        panel_corner_pattern = regex.compile(
            r"""(?x)
            # Match the pattern below
            (?> (?&DET_PANEL) / ) (?&COORD)
            \s = \s    # Match a equal sign with blank spaces on both sides
            (?&VALUE)  # Match the value of the coordinate

            (?(DEFINE)
                (?<DET_PANEL>
                    [0-9a-zA-Z]+
                )
                (?<COORD>
                    (?:corner_x)
                |   (?:corner_y)
                )
                (?<VALUE>
                    [-+]?         # Match a sign
                    (?>\d+)       # Match integer part
                    (?:\.\d*)?    # Match decimal part
                )
            )
            """)

        # Go through each line...
        geom_dict = {
            'panel_minmax' : {},
            'panel_orient' : {},
            'panel_corner' : {},
        }
        for geom_key, geom_value in geom_block.items():
            line = f"{geom_key} = {geom_value}"

            # Match a geom object...
            m = panel_minmax_pattern.match(line)
            if m is not None:
                # Fetch values...
                capture_dict = m.capturesdict()
                panel = capture_dict['DET_PANEL'][0]
                coord = capture_dict['COORD'][0]
                value = capture_dict['VALUE'][0]

                # Save values...
                if not panel in geom_dict['panel_minmax']:
                    geom_dict['panel_minmax'][panel] = {
                        'min_fs' : None,
                        'min_ss' : None,
                        'max_fs' : None,
                        'max_ss' : None,
                    }
                geom_dict['panel_minmax'][panel][coord] = int(value)

            # Match a geom object...
            m = panel_orient_pattern.match(line)
            if m is not None:
                # Fetch values...
                capture_dict = m.capturesdict()
                panel = capture_dict['DET_PANEL'][0]
                coord = capture_dict['COORD'][0]
                val_x = capture_dict['VALUE_X'][0]
                val_y = capture_dict['VALUE_Y'][0]

                # Save values...
                if not panel in geom_dict['panel_orient']:
                    geom_dict['panel_orient'][panel] = {
                        'fs' : None,
                        'ss' : None,
                    }
                geom_dict['panel_orient'][panel][coord] = (float(val_x), float(val_y))

            # Match a geom object...
            m = panel_corner_pattern.match(line)
            if m is not None:
                # Fetch values...
                capture_dict = m.capturesdict()
                panel = capture_dict['DET_PANEL'][0]
                coord = capture_dict['COORD'][0]
                value = capture_dict['VALUE'][0]

                # Save values...
                if not panel in geom_dict['panel_corner']:
                    geom_dict['panel_corner'][panel] = {
                        'corner_x' : None,
                        'corner_y' : None,
                    }
                geom_dict['panel_corner'][panel][coord] = float(value)

        idx_to_panel = {}
        panel_to_idx = {}
        cheetah2psana_geom_dict = {}
        for panel_idx, (panel_str, panel_minmax) in enumerate(geom_dict['panel_minmax'].items()):
            min_fs = panel_minmax['min_fs']
            min_ss = panel_minmax['min_ss']
            max_fs = panel_minmax['max_fs']
            max_ss = panel_minmax['max_ss']

            max_fs += 1
            max_ss += 1

            idx_to_panel[panel_idx] = panel_str
            panel_to_idx[panel_str] = panel_idx
            panel_key = panel_str
            if panel_key not in cheetah2psana_geom_dict: cheetah2psana_geom_dict[panel_key] = [min_fs, min_ss, max_fs, max_ss]
            panel_min_fs, panel_min_ss, panel_max_fs, panel_max_ss = cheetah2psana_geom_dict[panel_key]
            panel_min_fs = min(panel_min_fs, min_fs)
            panel_min_ss = min(panel_min_ss, min_ss)
            panel_max_fs = max(panel_max_fs, max_fs)
            panel_max_ss = max(panel_max_ss, max_ss)
            cheetah2psana_geom_dict[panel_key] = panel_min_fs, panel_min_ss, panel_max_fs, panel_max_ss

        self.geom_dict    = geom_dict
        self.idx_to_panel = idx_to_panel
        self.panel_to_idx = panel_to_idx
        self.cheetah2psana_geom_dict = cheetah2psana_geom_dict


    def convert_to_cheetah_img(self, img):
        W_cheetah, H_cheetah = list(self.cheetah2psana_geom_dict.values())[-1][-2:]
        cheetah_img = np.zeros((H_cheetah, W_cheetah), dtype = np.float32)

        # for (panel_idx, panel_str), (min_fs, min_ss, max_fs, max_ss) in enumerate(self.cheetah2psana_geom_dict.items()):
        for panel_str, (min_fs, min_ss, max_fs, max_ss) in self.cheetah2psana_geom_dict.items():
            H = max_ss - min_ss
            W = max_fs - min_fs
            panel_idx = self.panel_to_idx[panel_str]
            cheetah_img[min_ss:max_ss, min_fs:max_fs] = img[panel_idx, 0:H, 0:W]

        return cheetah_img


    def convert_to_psana_img(self, cheetah_img):
        # Figure out channel dimension...
        C = len(self.cheetah2psana_geom_dict)

        # Figure out spatial dimension...
        min_fs, min_ss, max_fs, max_ss = next(iter(self.cheetah2psana_geom_dict.values()))
        H = max_ss - min_ss
        W = max_fs - min_fs

        # Initialize a zero value image...
        img = np.zeros((C, H, W), dtype = np.float32)

        # for (panel_idx, panel_str), (min_fs, min_ss, max_fs, max_ss) in self.cheetah2psana_geom_dict.items():
        for panel_str, (min_fs, min_ss, max_fs, max_ss) in self.cheetah2psana_geom_dict.items():
            panel_idx = self.panel_to_idx[panel_str]
            img[panel_idx] = cheetah_img[min_ss:max_ss, min_fs:max_fs]

        return img


    def convert_to_cheetah_coords(self, peaks_psana_list):
        peaks_cheetah_list = [
            self.convert_to_cheetah_coord(idx_panel, y, x)
            for idx_panel, y, x in peaks_psana_list
        ]

        return peaks_cheetah_list


    def convert_to_cheetah_coord(self, idx_panel, y, x):
        panel_str = self.id_to_panel(idx_panel)
        min_fs, min_ss, max_fs, max_ss = self.cheetah2psana_geom_dict[panel_str]

        x += min_fs
        y += min_ss

        return idx_panel, y, x
