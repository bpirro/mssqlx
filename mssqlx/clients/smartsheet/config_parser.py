import configparser


def create_smartsheet_config():
    pass


def create_integration_config():
    config = configparser.ConfigParser()

    # Add sections and key-value pairs
    config['Application'] = {'debug': True, 'log_level': 'info'}
    config['Database'] = {
        'server_name': 'SAC-DATAANLTC',
        'database_name': 'BI_TEST',
        'schema_name': 'smartsheet',
        'table_name': 'CareSlCarouselsEos'

    }

    # Write the configuration to a file
    with open('smartsheet/care_sl_carousels_eos_int.ini', 'w') as configfile:
        config.write(configfile)


if __name__ == "__main__":
    create_integration_config()
