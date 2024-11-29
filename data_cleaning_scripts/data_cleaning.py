import os
import json

class CountryDataCleaner:
    def __init__(self):
        self.name_corrections = self.load_name_corrections()

    def load_name_corrections(self):
        """Loads name corrections from a JSON file."""
        try:
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            corrections_file = os.path.join(base_dir, "data", "name_corrections.json")
            with open(corrections_file, "r") as f:
                return json.load(f)

        except FileNotFoundError:
            print("Name corrections file not found. Proceeding without corrections.")
            return {}
        except json.JSONDecodeError as err:
            print(f"Error decoding JSON file: {err}. Proceeding without corrections.")
            return {}

    def clean_country_name(self, country_name):
        """Corrects country name based on the name corrections."""
        # Check if name_corrections is valid and use the default if not
        if not isinstance(self.name_corrections, dict):
            print("Name corrections is not a valid dictionary. Using original names.")
            return country_name
        return self.name_corrections.get(country_name, country_name)
