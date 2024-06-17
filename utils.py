# utils.py
import pandas as pd
import os

def read_csv(file_name):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, file_name)
    try:
        return pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"File not found: {file_name}")
    except pd.errors.EmptyDataError:
        print(f"No data in file: {file_name}")
    except pd.errors.ParserError:
        print(f"Error parsing file: {file_name}")

def contains_keywords(title, keywords):
    title_lower = title.lower()
    return any(keyword in title_lower for keyword in keywords)
