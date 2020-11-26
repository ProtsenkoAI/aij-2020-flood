from .. import utils


class ExtrConfigBuilder:
    def __init__(self):
        self.config = {}
        self.diff_params = None

    def set_diff_params(self, params):
        self.diff_params = params

    def build(self, meteo):
        all_cols = meteo.columns
        if self.diff_params:
            self.set_diff_config(all_cols)

        return self.config

    def update_with_config_and_cols(self, cols_config, cols):
        for col in cols:
            self.set_col_config(col, cols_config)

    def set_diff_config(self, all_cols, diff_pattern="diff"):
        diff_cols = utils.filter_by_substring(all_cols, diff_pattern)
        self.update_with_config_and_cols(self.diff_params, diff_cols)

    def set_col_config(self, col, col_config):
        self.config[col] = col_config
