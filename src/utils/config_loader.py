import yaml

class ConfigManager:
    def __init__(self, config_path):
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)

    def get(self, key, default=None):
        return self.config.get(key, default)

    def show_config(self):
        print("Current Configuration:")
        for key, value in self.config.items():
            print(f"{key}: {value}")
    '''
    def func(self, age, key, default=None):
        return self.config.get(age, {}).get(key, default)
    def show_config(self):
        print("Current Configuration:")
        for section, params in self.config.items():
            print(f"{age}:")
            for key, value in params.items():
                print(f"  {key}: {value}")
    '''

class flightreacts:
    def func1():
        return 1