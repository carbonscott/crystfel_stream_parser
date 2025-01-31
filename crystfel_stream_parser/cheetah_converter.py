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
            (   # Optional z component
                (?&VALUE_Z) z \s?
            )?

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
                (?<VALUE_Z>
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
                |   (?:corner_z)
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
                coord = capture_dict['COORD'    ][0]
                value = capture_dict['VALUE'    ][0]

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
                coord = capture_dict['COORD'    ][0]
                val_x = capture_dict['VALUE_X'  ][0]
                val_y = capture_dict['VALUE_Y'  ][0]
                val_z = capture_dict['VALUE_Z'  ][0] if len(capture_dict['VALUE_Z']) > 0 else 0

                # Save values...
                if not panel in geom_dict['panel_orient']:
                    geom_dict['panel_orient'][panel] = {
                        'fs' : None,
                        'ss' : None,
                    }
                geom_dict['panel_orient'][panel][coord] = (float(val_x), float(val_y), float(val_z))

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
                        'corner_z' : None,
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

            if panel_key not in cheetah2psana_geom_dict:
                cheetah2psana_geom_dict[panel_key] = [min_fs, min_ss, max_fs, max_ss]

            panel_min_fs, panel_min_ss, panel_max_fs, panel_max_ss = cheetah2psana_geom_dict[panel_key]
            panel_min_fs = min(panel_min_fs, min_fs)
            panel_min_ss = min(panel_min_ss, min_ss)
            panel_max_fs = max(panel_max_fs, max_fs)
            panel_max_ss = max(panel_max_ss, max_ss)
            cheetah2psana_geom_dict[panel_key] = panel_min_fs, panel_min_ss, panel_max_fs, panel_max_ss

        self.geom_dict               = geom_dict
        self.idx_to_panel            = idx_to_panel
        self.panel_to_idx            = panel_to_idx
        self.cheetah2psana_geom_dict = cheetah2psana_geom_dict


    def get_transform_matrix(self):
        panel_orient = self.geom_dict['panel_orient']    # (x, y, z)

        transform_matrix = {}
        for panel_str, orient in panel_orient.items():
            ss_orient = orient['ss']    # 'q0a0/fs': '+0.006140x +0.999981y',
            fs_orient = orient['fs']    # 'q0a0/ss': '-0.999981x +0.006140y',
            transform_matrix[panel_str] = np.array([fs_orient, ss_orient]).transpose(1, 0)

        return transform_matrix


    def get_transform_bias(self):
        panel_corner = self.geom_dict['panel_corner']    # (x, y, z)

        transform_bias = {}
        for panel_str, corner in panel_corner.items():
            corner_x = corner['corner_x']
            corner_y = corner['corner_y']
            corner_z = corner['corner_z'] if corner['corner_z'] is not None else 0
            transform_bias[panel_str] = np.array([corner_x, corner_y, corner_z]).reshape(-1, 1)

        return transform_bias


    def calculate_pixel_map(self, psana_img):
        panel_orient = self.geom_dict['panel_orient']    # (x, y, z)
        panel_corner = self.geom_dict['panel_corner']    # (x, y, z)
        lab_coords_x = np.zeros_like(psana_img)
        lab_coords_y = np.zeros_like(psana_img)
        lab_coords_z = np.zeros_like(psana_img)

        H, W = psana_img.shape[-2:]
        ss_range = np.arange(H)
        fs_range = np.arange(W)
        ss_coords, fs_coords = np.meshgrid(ss_range, fs_range, indexing='ij')
        original_coords = np.array([fs_coords, ss_coords])
        for panel_idx, panel_img in enumerate(psana_img):
            panel_str = self.idx_to_panel[panel_idx]

            corner_x = panel_corner[panel_str]['corner_x']
            corner_y = panel_corner[panel_str]['corner_y']
            corner_z = panel_corner[panel_str]['corner_z'] if panel_corner[panel_str]['corner_z'] is not None else 0
            corner   = np.array([corner_x, corner_y, corner_z]).reshape(-1, 1)

            ss_orient = panel_orient[panel_str]['ss']    # 'q0a0/fs': '+0.006140x +0.999981y',
            fs_orient = panel_orient[panel_str]['fs']    # 'q0a0/ss': '-0.999981x +0.006140y',
            transform_matrix = np.array([fs_orient, ss_orient]).transpose(1, 0)

            lab_coords = np.matmul(transform_matrix, original_coords.reshape(-1, H*W)) + corner    # (C, 2) @ (2, H*W) + (C, 1)
            panel_lab_coords_x, panel_lab_coords_y, panel_lab_coords_z = lab_coords.reshape(-1, H, W)    # (C, H*W) -> (C, H, W)

            lab_coords_x[panel_idx] = panel_lab_coords_x
            lab_coords_y[panel_idx] = panel_lab_coords_y
            lab_coords_z[panel_idx] = panel_lab_coords_z

        x_min_lab = lab_coords_x.min()
        y_min_lab = lab_coords_y.min()
        z_min_lab = lab_coords_z.min()

        pixel_map_x = lab_coords_x - x_min_lab
        pixel_map_y = lab_coords_y - y_min_lab
        pixel_map_z = lab_coords_z - z_min_lab

        return pixel_map_x, pixel_map_y, pixel_map_z


    def convert_pixel_map_from_psana_to_cheetah(self, psana_pixel_map):
        '''
        Arguments:
            psana_pixel_map: (P, H, W), ss aligns with H direction.
        '''
        return self.convert_to_cheetah_img(psana_pixel_map)


    def convert_to_detector_img(self, cheetah_img):
        '''
        Pixel map will be rounded for visualization.
        '''
        psana_img = self.convert_to_psana_img(cheetah_img)
        pixel_map_x, pixel_map_y, pixel_map_z = self.calculate_pixel_map(psana_img)

        pixel_map_x = np.round(pixel_map_x).astype(int)
        pixel_map_y = np.round(pixel_map_y).astype(int)
        pixel_map_z = np.round(pixel_map_z).astype(int)
        asmb_img  = np.zeros((pixel_map_x.max() - pixel_map_x.min() + 1,
                              pixel_map_y.max() - pixel_map_y.min() + 1,
                              pixel_map_z.max() - pixel_map_z.min() + 1,))
        asmb_img[pixel_map_x, pixel_map_y, pixel_map_z] = psana_img

        return asmb_img


    def convert_to_detector_coords(self, peak_positions, cheetah_img):
        geom_dict = self.cheetah2psana_geom_dict
        reduced_geom_dict = self.reduce_geom_to_p_level(geom_dict)

        min_fs, min_ss, max_fs, max_ss = next(iter(geom_dict.values()))
        H = max_ss - min_ss
        W = max_fs - min_fs

        psana_img = self.convert_to_psana_img(cheetah_img)
        pixel_map_x, pixel_map_y, pixel_map_z = self.calculate_pixel_map(psana_img)

        width = (len(geom_dict) // len(reduced_geom_dict)) // 2  # TODO

        local_coords = [ (int(width*(y // H) + (x // W)), int(y%H), int(x%W)) for y, x in peak_positions ]

        detector_coords = [(pixel_map_x[s,r,c], pixel_map_y[s,r,c]) for s,r,c in local_coords]

        return detector_coords


    def reduce_geom_to_p_level(self, data):
        reduced = {}
        for key, value in data.items():
            p_level = key.split('a')[0]  # Extract the p-level (e.g., 'p0', 'p1', etc.)

            if p_level not in reduced:
                # Initialize with the first 'a0' entry
                reduced[p_level] = list(value[:2]) + [0, 0]

            # Update max values if this is an 'a3' entry
            if key.endswith('a3'):
                reduced[p_level][2:] = value[2:]

        return reduced

    def convert_to_cheetah_img(self, img, reduces_geom=False):
        geom_dict = self.cheetah2psana_geom_dict
        if reduces_geom: geom_dict = self.reduce_geom_to_p_level(geom_dict)

        W_cheetah, H_cheetah = list(geom_dict.values())[-1][-2:]
        cheetah_img = np.zeros((H_cheetah, W_cheetah), dtype=np.float32)

        for panel_idx, (min_fs, min_ss, max_fs, max_ss) in enumerate(geom_dict.values()):
            H = max_ss - min_ss
            W = max_fs - min_fs
            cheetah_img[min_ss:max_ss, min_fs:max_fs] = img[panel_idx, 0:H, 0:W]

        return cheetah_img

    def convert_to_psana_img(self, cheetah_img, reduces_geom=False):
        geom_dict = self.cheetah2psana_geom_dict
        if reduces_geom: geom_dict = self.reduce_geom_to_p_level(geom_dict)

        # Figure out channel dimension...
        C = len(geom_dict)

        # Figure out spatial dimension...
        min_fs, min_ss, max_fs, max_ss = next(iter(geom_dict.values()))
        H = max_ss - min_ss
        W = max_fs - min_fs

        # Initialize a zero value image...
        img = np.zeros((C, H, W), dtype = np.float32)

        # for (panel_idx, panel_str), (min_fs, min_ss, max_fs, max_ss) in geom_dict.items():
        for panel_idx, (min_fs, min_ss, max_fs, max_ss) in enumerate(geom_dict.values()):
            img[panel_idx] = cheetah_img[min_ss:max_ss, min_fs:max_fs]

        return img


    def convert_to_cheetah_coords(self, peaks_psana_list):
        geom_dict = self.cheetah2psana_geom_dict
        geom_dict = self.reduce_geom_to_p_level(geom_dict)

        peaks_cheetah_list = [
            self.convert_to_cheetah_coord(idx_panel, y, x, geom_dict)
            for idx_panel, y, x in peaks_psana_list
        ]

        return peaks_cheetah_list

    def convert_to_cheetah_coord(self, idx_panel, y, x, geom_dict):
        min_fs, min_ss, max_fs, max_ss = list(geom_dict.values())[idx_panel]

        x += min_fs
        y += min_ss

        return idx_panel, y, x

    def convert_to_psana_coords(self, peaks_cheetah_list):
        geom_dict = self.cheetah2psana_geom_dict
        geom_dict = self.reduce_geom_to_p_level(geom_dict)

        peaks_psana_list = [
            self.convert_to_psana_coord(idx_panel, y, x, geom_dict)
            for idx_panel, y, x in peaks_cheetah_list
        ]

        return peaks_psana_list

    def convert_to_psana_coord(self, idx_panel, y, x, geom_dict):
        min_fs, min_ss, max_fs, max_ss = list(geom_dict.values())[idx_panel]

        x -= min_fs
        y -= min_ss

        return idx_panel, y, x
